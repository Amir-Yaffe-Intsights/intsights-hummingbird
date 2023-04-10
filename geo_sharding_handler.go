package hummingbird

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

func ZoneFieldPermutations() []string {
	return []string{
		"Zone",
		"zone",
		"accountZone",
		"location",
	}
}

func handleZoneFieldFromOplog(
	ParsedDoc bson.D,
) bson.D {
	ConvertedDoc := handleZoneField(ParsedDoc.Map())

	DocWithLocation := bson.D{}

	for key, value := range ConvertedDoc {
		DocWithLocation = append(DocWithLocation, bson.D{{key, value}}...)
	}

	return DocWithLocation
}

func handleZoneField(
	ParsedDoc bson.M,
) bson.M {
	for _, ZoneFieldPermutation := range ZoneFieldPermutations() {
		if val, ok := ParsedDoc[ZoneFieldPermutation]; ok {
			ZoneStr := strings.ToUpper(val.(string))

			switch ZoneStr {
			case "EU":
				ZoneStr = "ES"
			case "asia":
				ZoneStr = "JP"
			default:
			}

			ParsedDoc["location"] = ZoneStr
			delete(ParsedDoc, ZoneFieldPermutation)

			break

		}
	}

	return ParsedDoc
}
