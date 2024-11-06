package auction

import (
	"context"
	"fullcycle-auction_go/configuration/database/mongodb"
	"fullcycle-auction_go/internal/entity/auction_entity"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path"
	"testing"
	"time"
)

func TestAuctionAutoClose(t *testing.T) {
	dir := "./"
	envFilePath := path.Join(dir, "../../../../cmd/auction/.env")

	if err := godotenv.Load(envFilePath); err != nil {
		log.Fatal("Error trying to load env variables")
		return
	}

	ctx := context.Background()
	conn, err := mongodb.NewMongoDBConnection(ctx)
	if err != nil {
		log.Fatal("Error trying to connect mongodb")
		return
	}

	auction, _ := auction_entity.CreateAuction(
		"mouse",
		"peripherals",
		"mouse gamer rgb",
		auction_entity.New)

	ca := NewAuctionRepository(conn)
	ca.CreateAuction(ctx, auction)

	auctionInterval := os.Getenv("AUCTION_INTERVAL")
	durationAuction, err := time.ParseDuration(auctionInterval)
	if err != nil {
		log.Fatal("Error parsing durationAuction")
	}

	t.Log(auctionInterval)
	time.Sleep(durationAuction + time.Second*3)

	auctionDb, _ := ca.FindAuctionById(ctx, auction.Id)
	assert.Equal(t, auction_entity.Completed, auctionDb.Status)
}
