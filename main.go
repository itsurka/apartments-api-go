package main

import (
	json2 "encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"io"
	"itsurka/apartments-api/internal/dto"
	"itsurka/apartments-api/internal/helpers/dbhelper"
	eh "itsurka/apartments-api/internal/helpers/errhelper"
	"log"
	"net/http"
	"os"
	"time"
)

/**
Rest API
- GET http://localhost:8000/ping
- GET http://localhost:8000/apartments
- POST http://localhost:8000/apartments/import
*/

func main() {
	runWebServer()
	//apartments := getApartments(nil)
	//groupedApartments := groupApartmentsByUrl(apartments)
	//days, datasets := getChartData(groupedApartments)
	//fmt.Println(days, datasets)
}

func runWebServer() {
	http.HandleFunc("/ping", ping)
	http.HandleFunc("/apartments", getApartmentsRequest)
	http.HandleFunc("/apartments/import", handleImportApartmentsRequest)
	http.HandleFunc("/websocket/message/send", sendWebsocketMessage)

	err := http.ListenAndServe(":8000", nil)
	if errors.Is(err, http.ErrServerClosed) {

		fmt.Println("server closed")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	} else {
		fmt.Println("server started")
	}
}

func ping(wr http.ResponseWriter, request *http.Request) {
	_, err := io.WriteString(wr, "pong!")
	if err != nil {
		panic(err)
	}
}

func getApartments(request *http.Request) []dto.Apartment {
	err := godotenv.Load(".env")
	eh.FailOnError(err)

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
	// @todo remove!
	//priceChangerDirection := "down"

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
	eh.FailOnError(countErr)

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

func getApartmentsRequest(wr http.ResponseWriter, request *http.Request) {
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

func handleImportApartmentsRequest(wr http.ResponseWriter, request *http.Request) {
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	eh.FailOnError(err)
	defer conn.Close()

	ch, err := conn.Channel()
	eh.FailOnError(err)

	q, err := ch.QueueDeclare(
		"events",
		false,
		false,
		false,
		false,
		nil,
	)
	eh.FailOnError(err)

	body, err := json2.Marshal(Message{
		Version: "1",
		Event:   "api.apartments.import",
	})
	eh.FailOnError(err)

	publishErr := ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	eh.PanicOnError(publishErr)

	response := ApiResponse{
		Success: true,
	}
	json, jsonErr := json2.Marshal(response)
	eh.FailOnError(jsonErr)

	_, writeErr := io.WriteString(wr, string(json))
	eh.FailOnError(writeErr)
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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var wsConn = websocket.Conn{}

type WebsocketMessage struct {
	Event string
	//Version string
	//Data interface{}
}

func sendWebsocketMessage(wr http.ResponseWriter, request *http.Request) {
	conn, err := upgrader.Upgrade(wr, request, nil)
	eh.FailOnError(err)

	messageType, _, err := conn.NextReader()
	eh.FailOnError(err)
	w, err := conn.NextWriter(messageType)
	eh.FailOnError(err)

	messageDataBytes, err := json2.Marshal(WebsocketMessage{
		Event: "test",
	})
	eh.FailOnError(err)

	_, wsWriteErr := w.Write(messageDataBytes)
	eh.FailOnError(wsWriteErr)

	json, jsonErr := json2.Marshal(ApiResponse{
		Success: true,
	})
	eh.FailOnError(jsonErr)

	_, writeErr := io.WriteString(wr, string(json))
	eh.FailOnError(writeErr)
}
