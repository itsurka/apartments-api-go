package main

import (
	json2 "encoding/json"
	"errhlp"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"io"
	"itsurka/apartments-api/internal/dto"
	"itsurka/apartments-api/internal/helpers/dbhelper"
	"itsurka/apartments-api/internal/helpers/qhelper"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
)

/**
Rest API
- GET http://localhost:8000/ping
- GET http://localhost:8000/apartments
- POST http://localhost:8000/apartments/import
- GET ws://localhost:8000/ws
*/

var queue *qhelper.Queue

func main() {
	initEnvVars()
	runWebServer()
}

func initEnvVars() {
	err := godotenv.Load(".env")
	errhlp.Fatal(err)
}

func runWebServer() {
	apiMessageChannel := make(chan []byte)

	http.HandleFunc("/ping", ping)
	http.HandleFunc("/apartments", handleApartmentsRequest)
	http.HandleFunc("/apartments/import", handleImportApartmentsRequest(apiMessageChannel))
	http.HandleFunc("/apartments/test_import", handleTestImportApartmentsRequest(apiMessageChannel))
	http.HandleFunc("/ws", handleWebsocketRequest(apiMessageChannel))

	go func() {
		for {
			time.Sleep(3 * time.Second)
			fmt.Println("gr count", runtime.NumGoroutine())
		}
	}()

	err := http.ListenAndServe(":8000", nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Println("server closed")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	} else {
		log.Println("server started")
	}
}

func ping(wr http.ResponseWriter, request *http.Request) {
	_, err := io.WriteString(wr, "pong!")
	if err != nil {
		panic(err)
	}
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

func handleImportApartmentsRequest(wsMessageChannel chan []byte) func(w http.ResponseWriter, r *http.Request) {
	return func(wr http.ResponseWriter, request *http.Request) {
		wr.Header().Set("Access-Control-Allow-Origin", "*")
		wr.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		getQueue().Publish("apartments.pending", dto.QueueMessage{
			Version: "1",
			Event:   "api.apartments.import",
		})

		response := ApiResponse{
			Success: true,
		}
		json, jsonErr := json2.Marshal(response)
		errhlp.Fatal(jsonErr)

		_, writeErr := io.WriteString(wr, string(json))
		errhlp.Fatal(writeErr)

		writeToWsMessageChannel(wsMessageChannel, "api.apartments.import.received")
	}
}

func handleTestImportApartmentsRequest(wsMessageChannel chan []byte) func(w http.ResponseWriter, r *http.Request) {
	return func(wr http.ResponseWriter, request *http.Request) {
		wr.Header().Set("Access-Control-Allow-Origin", "*")
		wr.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		getQueue().Publish("apartments.pending", dto.QueueMessage{
			Version: "1",
			Event:   "api.apartments.import.test",
		})

		response := ApiResponse{
			Success: true,
		}
		json, jsonErr := json2.Marshal(response)
		errhlp.Fatal(jsonErr)

		_, writeErr := io.WriteString(wr, string(json))
		errhlp.Fatal(writeErr)

		writeToWsMessageChannel(wsMessageChannel, "api.apartments.test_import.received")
	}
}

func writeToWsMessageChannel(wsMessageChannel chan []byte, event string) {
	go func(wsMessageChannel chan []byte, event string) {
		message, _ := json2.Marshal(WebsocketMessage{
			Event: event,
		})

		wsMessageChannel <- message
	}(wsMessageChannel, event)
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
	if queue != nil {
		return queue
	}

	queue = &qhelper.Queue{
		ConnString: os.Getenv("RABBITMQ_URL"),
	}

	return queue
}

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
	//Version string
	//Data interface{}
}

func handleWebsocketRequest(apiMessageChannel chan []byte) func(w http.ResponseWriter, r *http.Request) {
	return func(wr http.ResponseWriter, request *http.Request) {
		log.Println("")
		log.Println("")
		log.Println("handle websocket request, goroutines", runtime.NumGoroutine())

		wsMessageChannel := make(chan []byte)
		log.Println("wsMessageChannel created", wsMessageChannel)
		closeAmqp := make(chan bool)
		close1 := make(chan bool)
		rabbitMqConnChannel := make(chan *amqp.Connection)

		conn, err := upgrader.Upgrade(wr, request, nil)
		errhlp.Fatal(err)

		exit := false
		deferCalled := false

		deferHandler := func(conn *websocket.Conn, closeAmqp chan bool, rabbitMqConnChannel chan *amqp.Connection, close1 chan bool) {
			if deferCalled {
				log.Println("deferHandler already called, skipping")
				return
			}
			deferCalled = true

			log.Println(">>>>   deferHandler   >>>>")
			closeAmqp <- true
			log.Println("empty rabbitMqConnChannel")
			errhlp.Fatal(conn.Close())
			log.Println("close websocket connection")
			close1 <- true
			log.Println("close1 set")
			log.Println("goroutines", runtime.NumGoroutine())

			exit = true
		}

		defer deferHandler(conn, closeAmqp, rabbitMqConnChannel, close1)

		conn.SetReadLimit(maxMessageSize)
		conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
		conn.SetCloseHandler(func(code int, text string) error {
			log.Println("websocket: close handler called", code, text)
			go deferHandler(conn, closeAmqp, rabbitMqConnChannel, close1)
			return errors.New(text)
		})

		go readWebsocket(conn)
		go sendPingToWebsocket(conn)

		go createRabbitMqConnection(os.Getenv("RABBITMQ_URL"), rabbitMqConnChannel, closeAmqp)
		go proxyApiToWebsocketMessageChannel(apiMessageChannel, wsMessageChannel, close1)
		go proxyFromQueueToMessageChannel(rabbitMqConnChannel, wsMessageChannel)

		go writeToWebsocketFromChannel(conn, wsMessageChannel)
		go waitWebsocketCloseMessage(conn)

		for {
			if exit {
				fmt.Println("ws request: exiting...")
				return
			}
		}
	}
}

func readWebsocket(conn *websocket.Conn) {
	for {
		mt, m, err := conn.ReadMessage()
		log.Println("read message", mt, string(m), err)

		if err != nil || mt == -1 {
			return
		}
	}
}

func sendPingToWebsocket(conn *websocket.Conn) {
	for {
		time.Sleep(5 * time.Second)

		conn.SetWriteDeadline(time.Now().Add(writeWait))
		if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
			log.Println("ping message error", err)
			return
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
	log.Println("websocket: writer started")

	defer func() {
		log.Println("gr done: writeToWebsocketFromChannel")
	}()

	for {
		message, ok := <-chanMessage
		if ok {
			log.Println("websocket: write message", string(message))
			writeMessageError := ws.WriteMessage(websocket.TextMessage, message)
			if writeMessageError != nil {
				log.Println("websocket: write message error", writeMessageError)
				return
			}
		} else {
			log.Println("websocket message channel closed")
			return
		}
	}
}

func waitWebsocketCloseMessage(ws *websocket.Conn) {
	log.Println("websocket: waiting for close message")

	for {
		messageType, message, messageErr := ws.ReadMessage()

		switch messageType {
		case -1, 8:
			log.Printf("websocket: connection closed by client with code %d and error: %s", messageType, messageErr)
			return
		default:
			log.Println("websocket: received message", string(message))
		}
	}
}

func proxyApiToWebsocketMessageChannel(apiMessageChannel chan []byte, wsMessageChannel chan []byte, close1 chan bool) {
	defer func() {
		log.Println("gr done: proxyApiToWebsocketMessageChannel")
	}()

	for {
		select {
		case message, ok := <-apiMessageChannel:
			if !ok {
				close(wsMessageChannel)
				return
			}
			wsMessageChannel <- message
		case <-close1:
			return
		}
	}
}

func proxyFromQueueToMessageChannel(rabbitMqConnChannel chan *amqp.Connection, wsMessageChannel chan []byte) {
	log.Println("rabbitmq: consumer started")

	defer func() {
		log.Println("gr done: proxyFromQueueToMessageChannel")
	}()

	queueChecked := false

	for {
		conn, ok := <-rabbitMqConnChannel
		if !ok {
			close(wsMessageChannel)
			return
		}

		log.Println("rabbitmq: connection read from channel")

		chanRabbitMQ, err := conn.Channel()
		errhlp.Fatal(err)

		log.Println("rabbitmq: channel connection ready")

		queueName := "apartments.done"
		if !queueChecked {
			_, err := chanRabbitMQ.QueueDeclare(
				queueName,
				true,
				false,
				false,
				false,
				nil,
			)
			errhlp.Fatal(err)
			queueChecked = true
		}

		messageDeliveryChannel, consumeErr := chanRabbitMQ.Consume(
			queueName,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		errhlp.Fatal(consumeErr)

		for d := range messageDeliveryChannel {
			log.Println("rabbitmq: read message", string(d.Body))
			wsMessageChannel <- d.Body
			log.Println("rabbitmq: message sent to channel")
		}
	}
}
