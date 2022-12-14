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
	"sync"
	"time"
)

/**
Rest API
- GET http://localhost:8000/ping?client_id=user1
- GET http://localhost:8000/apartments?client_id=user1
- POST http://localhost:8000/apartments/import?client_id=user1
- GET ws://localhost:8000/ws?client_id=user1
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
	m          sync.RWMutex
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
		app.m.Lock()
		for _, client := range app.webClients {
			if client == nil {
				continue
			}
			pt := "-"
			if client.wsPingAt != nil {
				pt = strconv.Itoa(int(time.Now().Sub(*client.wsPingAt).Seconds())) + "s"
			}
			amqpConnClosed := true
			var amqpConn *amqp.Connection
			amqpConn = nil
			if app.queue != nil && app.queue.Connection != nil {
				amqpConn = app.queue.Connection
				amqpConnClosed = app.queue.Connection.IsClosed()
			}
			var amqpChan *amqp.Channel
			amqpChan = nil
			if app.queue != nil && app.queue.Channel != nil {
				amqpChan = app.queue.Channel
			}

			log.Printf("client %s: grs: %d, step %s, ping: %s, ws: %v, amqp conn/chan: %v/%v, amqp state: %s\n", client.id, client.goroutinesCnt, client.step, pt, &client.wsConn, &amqpConn, &amqpChan, !amqpConnClosed)
		}
		app.m.Unlock()

		time.Sleep(10 * time.Second)
	}
}

func onBeforeRequest(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	initWebClient(rw, r)
}

func onAfterRequest(rw http.ResponseWriter, r *http.Request) {
}

func initWebClient(rw http.ResponseWriter, r *http.Request) {
	clientId := getClientId(r)

	if len(clientId) == 0 {
		rw.WriteHeader(400)
		rw.Write([]byte("ClientID not found"))
		return
	}

	client := getClientById(clientId)
	if client != nil {
		return
	}

	client = &WebClient{
		id:                clientId,
		step:              "init",
		closeGoroutinesCh: make(chan bool, 100),
		wsMessageCh:       make(chan []byte, 5),
		apiMessageCh:      make(chan []byte, 5),
	}

	addClient(client)
}

func getClientId(r *http.Request) string {
	return r.URL.Query().Get("client_id")
}

func getClientById(clientId string) *WebClient {
	app.m.Lock()
	defer app.m.Unlock()

	idx := slices.IndexFunc(app.webClients, func(webClient *WebClient) bool {
		return webClient != nil && webClient.id == clientId
	})

	if idx == -1 {
		return nil
	}

	return app.webClients[idx]
}

func addClient(client *WebClient) {
	app.m.Lock()
	defer app.m.Unlock()

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
	var queryCount string
	dateFrom := request.URL.Query().Get("date_from")
	dateTo := request.URL.Query().Get("date_to")

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
			"                                           and created_at BETWEEN $1 AND $2" +
			"                                           order by created_at asc limit 1)" +
			"                                          UNION ALL" +
			"                                          (select id, url, price_eur, created_at" +
			"                                           from apartments" +
			"                                           where price_eur > 0 and url = t.url" +
			"                                           and created_at BETWEEN $1 AND $2" +
			"                                           order by created_at desc limit 1)) t3) t2" +
			"                              where created_at BETWEEN $1 AND $2 AND t.price_eur " + sign + " t2.prev_price_eur and t2.prev_price_eur > 0)) " +
			"AND created_at BETWEEN $1 AND $2 " +
			"ORDER BY id ASC"

		queryCount = "SELECT COUNT(sq.id) FROM (" + query + ") sq"

	default:
		query = querySelect +
			"FROM apartments " +
			"WHERE created_at BETWEEN $1 AND $2 " +
			"ORDER BY id ASC"

		queryCount = "SELECT COUNT(id) FROM (" + query + ") sq"
	}

	var apartmentsCount int
	countErr := db.QueryRow(queryCount, dateFrom, dateTo).Scan(&apartmentsCount)
	errhlp.Fatal(countErr)

	log.Println("query, dateFrom, dateTo", query, dateFrom, dateTo)
	rows, err := db.Query(query, dateFrom, dateTo)
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
			"url":  dataset.Url,
			"data": dataset.Data,
			"img":  dataset.ImageUrl,
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
					dataset.Url = apartment.URL
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
	client := getClientById(getClientId(r))
	closed := make(chan bool)

	if client.step == "active" || client.step == "closing" {
		for client.step == "active" {
			time.Sleep(time.Second)
		}
		for client.step == "closing" {
			time.Sleep(time.Second)
		}
	}

	log.Println("============= ws: new request")
	client.step = "active"

	wsConn, wsConnErr := upgrader.Upgrade(rw, r, nil)
	errhlp.Fatal(wsConnErr)
	wsConn.SetReadLimit(maxMessageSize)
	wsConn.SetCloseHandler(func(code int, text string) error {
		message := websocket.FormatCloseMessage(code, "")
		wsConn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))
		closed <- true
		return nil
	})
	client.wsConn = wsConn

	go readWebsocket(client)
	go proxyQueueToWebsocket(client)
	go proxyMessagesToWebsocket(client)

	<-closed

	for i := 0; i < client.goroutinesCnt; i++ {
		client.closeGoroutinesCh <- true
	}

	client.step = "closing"

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

	log.Println("ws: closed func, client stat", client.step, client.goroutinesCnt)
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

		select {
		case <-client.closeGoroutinesCh:
			return
		case message := <-messageDeliveryChannel:
			log.Println("rabbitmq: read message", string(message.Body))
			writeToWebsocket(client.wsConn, message.Body)
			log.Println("rabbitmq: message sent to websocket")
		case <-t.C:
		}
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

			if mt == -1 || err != nil {
				log.Println("ws: error message", mt, m, err, client.wsConn.RemoteAddr().String(), client.wsConn.LocalAddr().String(), &client.wsConn)
				time.Sleep(2 * time.Second)
			}

			if mt == websocket.PingMessage || string(m) == "PING" {
				t := time.Now()
				client.wsPingAt = &t
			}
		}
	}
}

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

func proxyMessagesToWebsocket(client *WebClient) {
	client.goroutinesCnt++
	log.Println(">>>>> gr started: proxyMessagesToWebsocket", client.goroutinesCnt)

	defer func() {
		client.goroutinesCnt--
		log.Println("<<<<< gr done: proxyMessagesToWebsocket", client.goroutinesCnt)
	}()

	t := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-client.closeGoroutinesCh:
			return
		case message, ok := <-client.wsMessageCh:
			if !ok {
				return
			}
			writeToWebsocket(client.wsConn, message)
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
