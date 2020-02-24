package collector

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/gocolly/colly"
)

// GetWeatherInformation return DataFrame that is weather data for the specified period
func GetWeatherInformation(prefectureID int, blockID int, firstDate time.Time, lastDate time.Time) dataframe.DataFrame {
	// Initialize the variable that will be result for csv format, while inserting table header
	var csv string = "location,date,temperature,precipitation,humidity,wind_speed\n"

	var location string = strconv.Itoa(blockID)

	for date := firstDate; !date.After(lastDate); date = date.AddDate(0, 0, 1) {
		timer := time.Now()

		year, month, day := date.Date()

		var yearStr, monthStr, dayStr string
		yearStr = date.Format("2006")
		monthStr = date.Format("01")
		dayStr = date.Format("02")

		url := "https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no=" + strconv.Itoa(prefectureID) + "&block_no=" + strconv.Itoa(blockID) + "&year=" + yearStr + "&month=" + monthStr + "&day=" + dayStr + "&view="

		c := colly.NewCollector(
			// Visit restrictions
			colly.AllowedDomains("www.data.jma.go.jp"),
		)

		// Get the values of weather information
		c.OnHTML("table#tablefix1 tbody", func(te *colly.HTMLElement) {
			// Process for each row of table
			te.ForEach("tr", func(_ int, re *colly.HTMLElement) {
				var row []string
				var hour int
				// Process for each cell of table
				re.ForEach("td", func(_ int, de *colly.HTMLElement) {
					row = append(row, de.Text)
				})
				if len(row) != 0 {
					hour, _ = strconv.Atoi(row[0])
					row = []string{row[4], row[3], row[7], row[8]}
					for index, value := range row {
						value = regexp.MustCompile(`--`).ReplaceAllString(value, "0.0")
						value = regexp.MustCompile(` |^[^-]&^[^0-9]|[^0-9]$`).ReplaceAllString(value, "")
						if value == "" {
							value = "0"
						}
						row[index] = value
					}

					// Combine row value ​​with location's ID, date infomation and newline character into CSV string
					csv += location + "," + time.Date(year, month, day, hour-1, 0, 0, 0, time.Local).Format("2006-01-02 15:04:05") + "," + strings.Join(row, ",") + "\n"
				}
			})
		})

		// Before making a request print "Visiting ..."
		c.OnRequest(func(r *colly.Request) {
			fmt.Println("Visiting", r.URL.String())
		})

		// Start scraping
		c.Visit(url)

		// Sleep to follow the rule that it should leave at least 1 second interval
		elapsedTime := time.Now().Sub(timer).Milliseconds()
		if elapsedTime < 1000 {
			time.Sleep(time.Duration(1000-elapsedTime) * time.Millisecond)
		}
	}

	return (dataframe.ReadCSV(strings.NewReader(csv)))
}
