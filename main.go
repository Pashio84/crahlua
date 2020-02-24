package main

import (
	"fmt"
	"time"

	"local.packages/collector"
)

func main() {
	firstDate := time.Date(2020, 2, 1, 0, 0, 0, 0, time.Local)
	lastDate := time.Date(2020, 2, 10, 0, 0, 0, 0, time.Local)

	fmt.Println(collector.GetWeatherInformation(45, 47682, firstDate, lastDate))
}
