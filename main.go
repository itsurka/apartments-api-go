package main

import (
	json2 "encoding/json"
	"errhlp"
	"errors"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"golang.org/x/exp/slices"
	"io"
	"itsurka/apartments-api/internal/dto"
	"itsurka/apartments-api/internal/helpers/dbhelper"
	"itsurka/apartments-api/internal/helpers/qhelper"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

/**
Rest API
- GET http://localhost:8000/ping?client_id=user1
- GET http://localhost:8000/apartments
- POST http://localhost:8000/apartments/import
- GET ws://localhost:8000/ws
*/

var app App

type WebClient struct {
	id                string
	step              string
	wsPingAt          *time.Time
	wsConn            *websocket.Conn
	closeGoroutinesCh chan bool
	goroutinesCnt     int
	apiMessageCh      chan []byte
	wsMessageCh       chan []byte
}

type App struct {
	webClients []*WebClient
	amqpConn   *amqp.Connection
	queue      *qhelper.Queue
}

func init() {
	rand.Seed(time.Now().UnixNano())

	err := godotenv.Load(".env")
	errhlp.Fatal(err)

	app.webClients = make([]*WebClient, 3)
}

func main() {
	rh := func(reqHandler func(rw http.ResponseWriter, r *http.Request)) func(http.ResponseWriter, *http.Request) {
		return func(rw http.ResponseWriter, r *http.Request) {
			onBeforeRequest(rw, r)
			reqHandler(rw, r)
			onAfterRequest(rw, r)
		}
	}

	http.HandleFunc("/ping", rh(handlePingRequest))
	http.HandleFunc("/apartments", rh(handleApartmentsRequest))
	http.HandleFunc("/apartments/test_import", rh(handleTestImportApartmentsRequest))
	http.HandleFunc("/apartments/import", rh(handleImportApartmentsRequest))
	http.HandleFunc("/ws", rh(handleWebsocketRequest))

	go checkAppClients()

	err := http.ListenAndServe(":8000", nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Println("server closed")
	} else if err != nil {
		log.Printf("error starting server: %s\n", err)
		os.Exit(1)
	} else {
		log.Println("server started")
	}
}

func checkAppClients() {
	for {
		for _, client := range app.webClients {
			if client == nil {
				continue
			}
			pt := "-"
			if client.wsPingAt != nil {
				pt = strconv.Itoa(int(time.Now().Sub(*client.wsPingAt).Seconds())) + "s"
			}
			log.Printf("client %s: grs: %d, step %s, ping: %s, ws: %v, amqp conn/chan: %v/%v, amqp state: %s\n", client.id, client.goroutinesCnt, client.step, pt, &client.wsConn, &app.queue.Connection, &app.queue.Channel, !app.queue.Connection.IsClosed())
		}

		time.Sleep(10 * time.Second)
	}
}

func onBeforeRequest(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	initWebClient(rw, r)
}

func onAfterRequest(rw http.ResponseWriter, r *http.Request) {
	log.Println("after request app", app)
}

func initWebClient(rw http.ResponseWriter, r *http.Request) {
	clientId := getClientId(r)

	if len(clientId) == 0 {
		rw.WriteHeader(400)
		rw.Write([]byte("ClientID not found"))
		return
	}

	idx := slices.IndexFunc(app.webClients, func(webClient *WebClient) bool {
		return webClient != nil && webClient.id == clientId
	})

	if idx > -1 {
		return
	}

	client := &WebClient{
		id:                clientId,
		step:              "init",
		closeGoroutinesCh: make(chan bool, 100),
		apiMessageCh:      make(chan []byte, 5),
	}
	//app.webClients = append(app.webClients, client)

	addClient(client)
}

func getClientId(r *http.Request) string {
	return r.URL.Query().Get("client_id")
}

func getClientById(clientId string) *WebClient {
	idx := slices.IndexFunc(app.webClients, func(webClient *WebClient) bool {
		return webClient != nil && webClient.id == clientId
	})

	if idx == -1 {
		return nil
	}

	return app.webClients[idx]
}

func addClient(client *WebClient) {
	for i, c := range app.webClients {
		if c == nil {
			app.webClients[i] = client
			return
		}
	}

	app.webClients = append(app.webClients, client)
}

func handlePingRequest(rw http.ResponseWriter, r *http.Request) {
	client := getClientById(getClientId(r))
	t := time.Now()
	client.wsPingAt = &t

	_, err := io.WriteString(rw, "pong from client_id: "+client.id)
	errhlp.Panic(err)
}

func getApartments(request *http.Request) []dto.Apartment {
	dbConfig := dbhelper.DbConfig{
		os.Getenv("DB_DRIVER"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME"),
	}
	db := dbhelper.GetConnection(dbConfig)

	querySelect := "SELECT id,url,title,description,price_eur,price_usd,price_leu,price_square_meter_eur,location," +
		"last_updated,page_views,seller_login,seller_phone,image_urls,to_char(created_at, 'YYYY-MM-DD') " +
		"as created_at,updated_at,unavailable_from "
	var query string
	var countQuery string

	priceChangerDirection := request.URL.Query().Get("price_changed")

	switch priceChangerDirection {
	case "down", "up":
		var sign string
		if priceChangerDirection == "down" {
			sign = "<"
		} else {
			sign = ">"
		}

		query = querySelect +
			"from apartments " +
			"where url IN (select distinct t.url" +
			"              from apartments t" +
			"              where t.url IN (select url" +
			"                              from (select price_eur," +
			"                                           lag(price_eur) over (order by created_at asc) as prev_price_eur," +
			"                                           url" +
			"                                    from ((select id, url, price_eur, created_at" +
			"                                           from apartments" +
			"                                           where price_eur > 0 and url = t.url" +
			"                                           order by created_at asc limit 1)" +
			"                                          UNION ALL" +
			"                                          (select id, url, price_eur, created_at" +
			"                                           from apartments" +
			"                                           where price_eur > 0 and url = t.url" +
			"                                           order by created_at desc limit 1)) t3) t2" +
			"                              where t.price_eur " + sign + " t2.prev_price_eur and t2.prev_price_eur > 0)) " +
			"ORDER BY id ASC"

		countQuery = "SELECT COUNT(sq.id) FROM (" + query + ") sq"

	default:
		query = querySelect +
			"FROM apartments " +
			"ORDER BY id ASC"

		countQuery = "SELECT COUNT(id) FROM apartments"
	}

	var apartmentsCount int
	countErr := db.QueryRow(countQuery).Scan(&apartmentsCount)
	errhlp.Fatal(countErr)

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	apartments := make([]dto.Apartment, apartmentsCount)

	for i := 0; rows.Next(); i++ {
		var (
			id                  int
			url                 string
			title               string
			description         string
			priceEur            float64
			priceUsd            float64
			priceLeu            float64
			priceSquareMeterEur float64
			location            string
			lastUpdated         string
			pageViews           int
			sellerLogin         string
			sellerPhone         string
			imageUrls           string
			createdAt           string
			updatedAt           string
			unavailableFrom     *string
		)

		scanErr := rows.Scan(
			&id,
			&url,
			&title,
			&description,
			&priceEur,
			&priceUsd,
			&priceLeu,
			&priceSquareMeterEur,
			&location,
			&lastUpdated,
			&pageViews,
			&sellerLogin,
			&sellerPhone,
			&imageUrls,
			&createdAt,
			&updatedAt,
			&unavailableFrom,
		)
		if scanErr != nil {
			panic(scanErr)
		}

		apartments[i] = dto.Apartment{
			id,
			url,
			title,
			description,
			priceEur,
			priceUsd,
			priceLeu,
			priceSquareMeterEur,
			location,
			lastUpdated,
			pageViews,
			sellerLogin,
			sellerPhone,
			imageUrls,
			createdAt,
			updatedAt,
			unavailableFrom,
		}
	}

	return apartments
}

type Map map[string]interface{}
type ChartData map[string]interface{}

func handleApartmentsRequest(wr http.ResponseWriter, request *http.Request) {
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	apartments := getApartments(request)
	groupedApartments := groupApartmentsByUrl(apartments)
	days, datasets := getChartData(groupedApartments)

	daysStr := make([]string, len(days))
	for i, v := range days {
		daysStr[i] = v.Format("2006-01-02")
	}

	datasetsStr := []Map{}

	for _, dataset := range datasets {
		datasetsStr = append(datasetsStr, Map{
			"name": dataset.Label,
			"data": dataset.Data,
		})
	}

	chartData := ChartData{
		"categories": daysStr,
		"series":     datasetsStr,
	}

	result, encodeErr := json2.Marshal(chartData)
	if encodeErr != nil {
		log.Fatal(encodeErr)
	}

	io.WriteString(wr, string(result))
}

type Message struct {
	Version string
	Event   string
	Data    interface{}
}

type ApiResponse struct {
	Success bool
	Data    interface{}
}

func handleImportApartmentsRequest(rw http.ResponseWriter, r *http.Request) {
	getQueue().Publish("apartments.pending", dto.QueueMessage{
		Version: "1",
		Event:   "api.apartments.import",
	})

	response := ApiResponse{
		Success: true,
	}
	json, jsonErr := json2.Marshal(response)
	errhlp.Fatal(jsonErr)

	_, writeErr := io.WriteString(rw, string(json))
	errhlp.Fatal(writeErr)

	client := getClientById(getClientId(r))

	writeToWsMessageChannel(client.wsMessageCh, "api.apartments.import.received")
}

func handleTestImportApartmentsRequest(rw http.ResponseWriter, r *http.Request) {
	getQueue().Publish("apartments.pending", dto.QueueMessage{
		Version: "1",
		Event:   "api.apartments.import.test",
	})

	json, jsonErr := json2.Marshal(ApiResponse{
		Success: true,
	})
	errhlp.Fatal(jsonErr)

	_, writeErr := io.WriteString(rw, string(json))
	errhlp.Fatal(writeErr)

	client := getClientById(getClientId(r))

	writeToWsMessageChannel(client.apiMessageCh, "api.apartments.test_import.received")
}

func writeToWsMessageChannel(wsMessageCh chan []byte, event string) {
	go func(wsMessageCh chan []byte, event string) {
		message, _ := json2.Marshal(WebsocketMessage{
			Event: event,
		})

		log.Println("proxy message api -> wsMessageCh =1", event, &wsMessageCh)
		wsMessageCh <- message
		log.Println("proxy message api -> wsMessageCh =2")
	}(wsMessageCh, event)
}

func groupApartmentsByUrl(apartments []dto.Apartment) map[string][]dto.Apartment {
	groupedApartments := map[string][]dto.Apartment{}

	for _, app := range apartments {
		_, exists := groupedApartments[app.URL]
		if exists == false {
			groupedApartments[app.URL] = []dto.Apartment{}
		}

		groupedApartments[app.URL] = append(groupedApartments[app.URL], app)
	}

	return groupedApartments
}

func getChartData(groupedApartments map[string][]dto.Apartment) ([]time.Time, []dto.ApartmentDataset) {
	var (
		minDay *time.Time
		maxDay *time.Time
	)
	for _, apartments := range groupedApartments {
		for _, apartment := range apartments {
			if apartment.Id == 0 {
				break
			}

			createdAt, _ := time.Parse("2006-01-02", apartment.CreatedAt)

			if minDay == nil || minDay.After(createdAt) {
				minDay = &createdAt
			}
			if maxDay == nil || maxDay.Before(createdAt) {
				maxDay = &createdAt
			}
		}
	}

	days := []time.Time{}
	datasets := []dto.ApartmentDataset{}

	if minDay == nil || maxDay == nil {
		return days, datasets
	}

	loc, _ := time.LoadLocation("UTC")
	currentDay := time.Date(minDay.Year(), minDay.Month(), minDay.Day(), 0, 0, 0, 0, loc)

	for i := 0; ; i++ {
		if currentDay.After(*maxDay) {
			break
		}

		days = append(days, currentDay)
		currentDay = currentDay.AddDate(0, 0, 1)
	}

	for _, apartments := range groupedApartments {
		dataset := dto.ApartmentDataset{}

		for _, day := range days {
			var dayPrice float64

			for _, apartment := range apartments {
				if dataset.Label == "" {
					dataset.Label = apartment.Title
				}

				date, _ := time.Parse("2006-01-02", apartment.CreatedAt)
				a := date.Unix()
				b := day.Unix()
				if a == b {
					dayPrice = apartment.PriceEur
					break
				}
			}

			dataset.Data = append(dataset.Data, dayPrice)
		}

		// set previous prices instead of zeros
		var prevPrice float64
		var price float64

		for i := 0; i < len(dataset.Data); i++ {
			price = dataset.Data[i]
			if price == 0 && prevPrice > 0 {
				dataset.Data[i] = prevPrice
			}

			if price > 0 {
				prevPrice = price
			}
		}

		prevPrice = 0
		for i := len(dataset.Data) - 1; i > -1; i-- {
			price = dataset.Data[i]
			if price == 0 && prevPrice > 0 {
				dataset.Data[i] = prevPrice
			}

			if price > 0 {
				prevPrice = price
			}
		}

		datasets = append(datasets, dataset)
	}

	return days, datasets
}

func getQueue() *qhelper.Queue {
	if app.queue != nil {
		return app.queue
	}

	app.queue = &qhelper.Queue{
		ConnString: os.Getenv("RABBITMQ_URL"),
	}

	return app.queue
}

//var conn *websocket.Conn
//var amqpConn *amqp.Connection
//var errError error

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type WebsocketMessage struct {
	Event string
}

func handleWebsocketRequest(rw http.ResponseWriter, r *http.Request) {
	log.Println("=========================================")
	log.Println("ws: new request")

	client := getClientById(getClientId(r))
	closed := make(chan bool)

	log.Println("ws: client stat =1", client.step, client.goroutinesCnt)

	if client.step == "active" || client.step == "closing" {
		log.Println("step =1: ", client.step)
		for client.step == "active" {
			time.Sleep(time.Second)
		}
		log.Println("step =2: ", client.step)
		for client.step == "closing" {
			time.Sleep(time.Second)
		}
		log.Println("step =3: ", client.step)

		// @todo amqp connection check after ws reconnection...

		//if client.goroutinesCnt > 0 {
		//	for i := 0; i < client.goroutinesCnt; i++ {
		//		client.closeGoroutinesCh <- true
		//	}
		//}
		//for {
		//	log.Println("=============== closing prev connection...", client.step, client.goroutinesCnt)
		//	//errhlp.Fatal(client.wsConn.Close())
		//	time.Sleep(time.Second)
		//
		//	if client.step == "closed" || client.goroutinesCnt == 0 {
		//		log.Println("=============== closing prev connection: step closed...")
		//		time.Sleep(time.Second)
		//		break
		//	}
		//
		//	log.Println("ws: client stat on closing...", client.step, client.goroutinesCnt)
		//}
	}

	log.Println("ws: client stat =2", client.step, client.goroutinesCnt)
	client.step = "active"

	wsConn, wsConnErr := upgrader.Upgrade(rw, r, nil)
	errhlp.Fatal(wsConnErr)
	wsConn.SetReadLimit(maxMessageSize)
	//wsConn.SetReadDeadline(time.Now().Add(pongWait))
	//wsConn.SetPongHandler(func(string) error { wsConn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	wsConn.SetCloseHandler(func(code int, text string) error {
		log.Println("ws: close handler called =1", code, text, "client", client.goroutinesCnt)
		message := websocket.FormatCloseMessage(code, "")
		wsConn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))
		log.Println("ws: close handler called =2")
		closed <- true
		log.Println("ws: close handler called =3")
		return nil
	})
	client.wsConn = wsConn
	log.Println("ws: client stat =3", client.step, client.goroutinesCnt)

	go readWebsocket(client)
	go proxyQueueToWebsocket(client)
	go proxyApiToWebsocket(client)

	log.Println("ws: client stat =4", client.step, client.goroutinesCnt)

	<-closed

	log.Println("ws: client stat =5 closed signal", client.step, client.goroutinesCnt)

	for i := 0; i < client.goroutinesCnt; i++ {
		log.Println("XXX set close signal closeGoroutinesCh")
		client.closeGoroutinesCh <- true
	}

	client.step = "closing"
	//log.Println("========== closing websocket...")
	//errhlp.Fatal(wsConn.Close())
	//log.Println("========== websocket closed!")

	// waiting goroutines close
	for {
		if client.goroutinesCnt > 0 {
			time.Sleep(500 * time.Millisecond)
			log.Println("waiting goroutines close", len(client.closeGoroutinesCh), "close ch", client.goroutinesCnt, "gr count")
			continue
		}
		time.Sleep(time.Second)
		break
	}

	client.step = "closed"

	log.Println("ws: client stat =6 closed func", client.step, client.goroutinesCnt)
}

func getAmqpConnection() *amqp.Connection {
	if app.amqpConn != nil && !app.amqpConn.IsClosed() {
		return app.amqpConn
	}

	amqpConn, errError := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	errhlp.Fatal(errError)

	app.amqpConn = amqpConn

	return app.amqpConn
}

func proxyQueueToWebsocket(client *WebClient) {
	client.goroutinesCnt++
	log.Println(">>>>> gr started: proxyQueueToWebsocket", client.goroutinesCnt)

	queueName := "apartments.done"
	//rabbitMqChan := getRabbitMqChannel(getAmqpConnection(), queueName)

	t := time.NewTicker(2 * time.Second)

	log.Println("rabbitmq: consuming...")
	rabbitMqChan := getRabbitMqChannel(getAmqpConnection(), queueName)
	messageDeliveryChannel, consumeErr := rabbitMqChan.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	errhlp.Fatal(consumeErr)

	defer func() {
		client.goroutinesCnt--
		rabbitMqChan.Close()
		log.Println("<<<<< gr done: proxyQueueToWebsocket", client.goroutinesCnt)
	}()

	for {
		//log.Println("rabbitmq: messageDeliveryChannel len is %d", len(messageDeliveryChannel))
		//log.Println("rabbitmq: before select...")

		select {
		case <-client.closeGoroutinesCh:
			return
		//case <-rabbitMqChan.NotifyClose(chanConnErr):
		//	rabbitMqChan = getRabbitMqChannel(amqpConn, queueName)
		case message := <-messageDeliveryChannel:
			log.Println("rabbitmq: read message", string(message.Body))
			writeToWebsocket(client.wsConn, message.Body)
			log.Println("rabbitmq: message sent to websocket")
		case <-t.C:
			//log.Println("rabbitmq: waiting...")
		}

		//log.Println("rabbitmq: after select...")
	}
}

func getRabbitMqChannel(rabbitMqConn *amqp.Connection, queueName string) *amqp.Channel {
	chanRabbitMQ, err := rabbitMqConn.Channel()
	errhlp.Fatal(err)

	_, queueErr := chanRabbitMQ.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	errhlp.Fatal(queueErr)

	return chanRabbitMQ
}

func readWebsocket(client *WebClient) {
	client.goroutinesCnt++
	log.Println(">>>>> gr started: readWebsocket", client.goroutinesCnt, client.wsConn.RemoteAddr().String(), client.wsConn.LocalAddr().String())

	defer func() {
		client.goroutinesCnt--
		log.Println("<<<<< gr done: readWebsocket", client.goroutinesCnt)
	}()

	for {
		select {
		case <-client.closeGoroutinesCh:
			return
		default:
			mt, m, err := client.wsConn.ReadMessage()

			//if len(m) > 0 && string(m) == "PING" {
			//	log.Println("ws: new message", string(m))
			//}

			if mt == -1 || err != nil {
				log.Println("ws: error message", mt, m, err, client.wsConn.RemoteAddr().String(), client.wsConn.LocalAddr().String(), &client.wsConn)
				//<-client.closeGoroutinesCh
				//return
				time.Sleep(2 * time.Second)
			}

			if mt == websocket.PingMessage || string(m) == "PING" {
				t := time.Now()
				client.wsPingAt = &t
			}
		}
	}
}

//func pingToWebsocket(conn *websocket.Conn, wsGoroutinesClose chan bool) {
//	client.goroutinesCnt++
//	log.Println("gr started: pingToWebsocket")
//
//	defer func() {
//		wsGoroutines--
//		wsGoroutineList = append(wsGoroutineList, "pingToWebsocket --deleted")
//		log.Println("gr done: pingToWebsocket")
//	}()
//
//	for {
//		select {
//		case <-wsGoroutinesClose:
//			return
//		default:
//			time.Sleep(5 * time.Second)
//
//			conn.SetWriteDeadline(time.Now().Add(writeWait))
//			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
//				log.Println("gr: pingToWebsocket error", err)
//				return
//			}
//		}
//	}
//}

func createRabbitMqConnection(url string, rabbitMqConnChannel chan *amqp.Connection, closeAmqp chan bool) {
	defer func() {
		log.Println("gr done: createRabbitMqConnection, rabbitMqConnChannel", rabbitMqConnChannel)
	}()

	for {
		select {
		case <-closeAmqp:
			close(rabbitMqConnChannel)
			return

		default:
			log.Println("rabbitmq: create connection")
			conn, err := amqp.Dial(url)
			errhlp.Panic(err)

			rabbitMqConnChannel <- conn
			log.Println("rabbitmq: connection sent to channel")

			defer func(conn *amqp.Connection) {
				errhlp.Panic(conn.Close())
			}(conn)
		}
	}
}

func checkRabbitMqConnection(rabbitMqConnChannel chan *amqp.Connection) {
	log.Println("rabbitmq: check connection")
	for {
		select {
		case conn, ok := <-rabbitMqConnChannel:
			log.Println("checkRabbitMqConnection conn and ok", conn, ok)
		}
	}
}

func createRabbitMqConnectionChannel(conn *amqp.Connection, connectionChannel chan *amqp.Connection) (*amqp.Channel, error) {
	log.Println("rabbitmq: create connection channel")
	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	channel := ch
	go func() {
		for {
			log.Println("rabbitmq: channel monitoring started")

			reason, ok := <-channel.NotifyClose(make(chan *amqp.Error))
			log.Printf("rabbitmq: channel closed. Reason: %v, ok: %v\n", reason, ok)

			for {
				time.Sleep(1 * time.Second)
				conn := <-connectionChannel

				if conn != nil {
					ch, err := conn.Channel()
					if err == nil {
						log.Println("rabbitmq: channel reconnected")
						channel = ch

						//Non-blocking push new connection back to connectionChannel to chain new connection to other RabbitMQ channels
						select {
						case connectionChannel <- conn:
						default:
						}

						break
					}

					log.Printf("rabbitmq: can't reconnect channel. Error: %v\n", err)
				}
			}
		}
	}()

	return channel, nil
}

func writeToWebsocketFromChannel(ws *websocket.Conn, chanMessage chan []byte) {
	log.Println("ws: writer started")

	defer func() {
		log.Println("gr done: writeToWebsocketFromChannel")
	}()

	for {
		message, ok := <-chanMessage
		if ok {
			log.Println("ws: write message", string(message))
			writeMessageError := ws.WriteMessage(websocket.TextMessage, message)
			if writeMessageError != nil {
				log.Println("ws: write message error", writeMessageError)
				return
			}
		} else {
			log.Println("websocket message channel closed")
			return
		}
	}
}

func writeToWebsocket(ws *websocket.Conn, message []byte) {
	log.Println("ws: write message", string(message))
	err := ws.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		errhlp.Fatal(err)
	}
}

func waitWebsocketCloseMessage(ws *websocket.Conn) {
	log.Println("ws: waiting for close message")

	for {
		messageType, message, messageErr := ws.ReadMessage()

		switch messageType {
		case -1, 8:
			log.Printf("ws: connection closed by client with code %d and error: %s", messageType, messageErr)
			return
		default:
			log.Println("ws: received message", string(message))
		}
	}
}

//func proxyApiToWebsocketMessageChannel(apiMessageCh chan []byte, amqpConn *amqp.Connection) {
//	defer func() {
//		log.Println("gr done: proxyApiToWebsocketMessageChannel")
//	}()
//
//	for {
//		select {
//		case message, ok := <-apiMessageCh:
//			if !ok {
//				close(wsMessageChannel)
//				return
//			}
//			wsMessageChannel <- message
//		case <-close1:
//			return
//		}
//	}
//}

func proxyApiToWebsocket(client *WebClient) {
	client.goroutinesCnt++
	log.Println(">>>>> gr started: proxyApiToWebsocket", client.goroutinesCnt)

	defer func() {
		client.goroutinesCnt--
		log.Println("<<<<< gr done: proxyApiToWebsocket", client.goroutinesCnt)
	}()

	t := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-client.closeGoroutinesCh:
			return
		case message, ok := <-client.apiMessageCh:
			if !ok {
				return
			}
			writeToWebsocket(client.wsConn, message)
		case <-t.C:
		}
	}
}

func getRandomString(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}

	return string(b)
}

func inArray(items []string, item string) bool {
	for _, v := range items {
		if v == item {
			return true
		}
	}

	return false
}
