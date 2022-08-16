package dto

type Apartment struct {
	Id                  int
	URL                 string
	Title               string
	Desc                string
	PriceEur            float64
	PriceUsd            float64
	PriceLeu            float64
	PriceSquareMeterEur float64
	Location            string
	LastUpdated         string
	PageViews           int
	SellerLogin         string
	SellerPhone         string
	ImageUrls           string
	CreatedAt           string
	UpdatedAt           string
	UnavailableFrom     *string
}
