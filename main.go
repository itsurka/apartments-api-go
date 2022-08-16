package main

import (
	json2 "encoding/json"
	"errors"
	"fmt"
	"io"
	"itsurka/apartments-api/internal/dto"
	"itsurka/apartments-api/internal/helpers/dbhelper"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	http.HandleFunc("/ping", ping)

	http.HandleFunc("/apartments", getApartmentsRequest)

	err := http.ListenAndServe(":3333", nil)
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
	io.WriteString(wr, "pong!")
}

func getApartments() []dto.Apartment {
	dbConfig := dbhelper.DbConfig{
		"postgres",
		"localhost",
		5432,
		"test",
		"test",
		"go_web_parser",
	}
	db := dbhelper.GetConnection(dbConfig)

	var apartmentsCount int
	countErr := db.QueryRow("SELECT COUNT(id) FROM apartments").Scan(&apartmentsCount)
	if countErr != nil {
		panic(countErr)
	}
	//const apartmentsCount2 = apartmentsCount
	// @todo defer close?

	rows, err := db.Query("SELECT id,url,title,description,price_eur,price_usd,price_leu,price_square_meter_eur,location,last_updated,page_views,seller_login,seller_phone,image_urls,to_char(created_at, 'YYYY-MM-DD') as created_at,updated_at,unavailable_from FROM apartments ORDER BY id ASC")
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

	apartments := getApartments()
	groupedApartments := groupApartmentsByUrl(apartments)
	days, datasets := getChartData(groupedApartments)

	daysStr := make([]string, len(days))
	for i, v := range days {
		daysStr[i] = v.Format("2006-01-02")
	}

	datasetsStr := []Map{}

	for _, dataset := range datasets {
		datasetsStr = append(datasetsStr, Map{
			"label": dataset.Label,
			"data":  dataset.Data,
		})
	}

	chartData := ChartData{
		"labels":   daysStr,
		"datasets": datasetsStr,
	}

	result, encodeErr := json2.Marshal(chartData)
	if encodeErr != nil {
		log.Fatal(encodeErr)
	}

	io.WriteString(wr, string(result))
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
	loc, _ := time.LoadLocation("UTC")
	currentDay := time.Date(minDay.Year(), minDay.Month(), minDay.Day(), 0, 0, 0, 0, loc)

	for i := 0; ; i++ {
		if currentDay.After(*maxDay) {
			break
		}

		days = append(days, currentDay)
		currentDay = currentDay.AddDate(0, 0, 1)
	}

	datasets := []dto.ApartmentDataset{}

	for _, apartments := range groupedApartments {
		dataset := dto.ApartmentDataset{}
		var dayPrice float64
		var prevDayPrice float64

		for _, day := range days {

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

			if dayPrice > 0 {
				dataset.Data = append(dataset.Data, dayPrice)
				prevDayPrice = dayPrice
			} else {
				dataset.Data = append(dataset.Data, prevDayPrice)
			}
		}

		datasets = append(datasets, dataset)
	}

	return days, datasets
}
