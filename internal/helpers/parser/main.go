package parser

import (
	"bytes"
	"github.com/PuerkitoBio/goquery"
)

func ParsePageData(pageData []byte) goquery.Document {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(pageData))
	if err != nil {
		panic(err)
	}

	return *doc
}
