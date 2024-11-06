package auction

import (
	"context"
	"fmt"
	"fullcycle-auction_go/configuration/logger"
	"fullcycle-auction_go/internal/entity/auction_entity"
	"fullcycle-auction_go/internal/internal_error"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type AuctionEntityMongo struct {
	Id          string                          `bson:"_id"`
	ProductName string                          `bson:"product_name"`
	Category    string                          `bson:"category"`
	Description string                          `bson:"description"`
	Condition   auction_entity.ProductCondition `bson:"condition"`
	Status      auction_entity.AuctionStatus    `bson:"status"`
	Timestamp   int64                           `bson:"timestamp"`
}

type AuctionRepository struct {
	Collection        *mongo.Collection
	AuctionsAutoClose map[string]auction_entity.AuctionStatus
	CloseMutex        *sync.Mutex
}

func NewAuctionRepository(database *mongo.Database) *AuctionRepository {
	return &AuctionRepository{
		Collection:        database.Collection("auctions"),
		AuctionsAutoClose: make(map[string]auction_entity.AuctionStatus),
		CloseMutex:        &sync.Mutex{},
	}
}

func (ar *AuctionRepository) CreateAuction(
	ctx context.Context,
	auctionEntity *auction_entity.Auction) *internal_error.InternalError {

	auctionEntityMongo := &AuctionEntityMongo{
		Id:          auctionEntity.Id,
		ProductName: auctionEntity.ProductName,
		Category:    auctionEntity.Category,
		Description: auctionEntity.Description,
		Condition:   auctionEntity.Condition,
		Status:      auctionEntity.Status,
		Timestamp:   auctionEntity.Timestamp.Unix(),
	}
	_, err := ar.Collection.InsertOne(ctx, auctionEntityMongo)
	if err != nil {
		logger.Error("Error trying to insert auction", err)
		return internal_error.NewInternalServerError("Error trying to insert auction")
	}

	ar.CloseMutex.Lock()
	err = ar.autoClose(ctx)
	ar.CloseMutex.Unlock()

	if err != nil {
		return internal_error.NewInternalServerError("Failed to initiate auto-close process")
	}

	return nil
}

func (ar *AuctionRepository) autoClose(ctx context.Context) error {
	openAuctions, err := ar.FindOpenAuctions(ctx)
	if err != nil {
		return err
	}

	for _, auctionEntity := range openAuctions {
		timeUntilClose := calculateAuctionEndTime(auctionEntity)

		if timeUntilClose <= 0 {
			err := ar.closeAuction(ctx, auctionEntity)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to close auction %s immediately", auctionEntity.Id), err)
			}
			continue
		}

		ar.CloseMutex.Lock()
		_, okStatus := ar.AuctionsAutoClose[auctionEntity.Id]
		if okStatus {
			ar.CloseMutex.Unlock()
			continue
		}

		ar.AuctionsAutoClose[auctionEntity.Id] = auction_entity.Active
		ar.CloseMutex.Unlock()

		timer := time.NewTimer(timeUntilClose)

		go func(auction auction_entity.Auction) {
			defer timer.Stop()

			select {
			case <-ctx.Done():
				logger.Info(fmt.Sprintf("Auction closing for %s cancelled due to context cancellation", auction.Id))
				return
			case <-timer.C:
				ar.CloseMutex.Lock()
				defer ar.CloseMutex.Unlock()

				err := ar.closeAuction(ctx, auction)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to close auction %s automatically", auction.Id), err)
				}
				delete(ar.AuctionsAutoClose, auction.Id)
			}
		}(auctionEntity)
	}

	return nil
}

func getAuctionInterval() time.Duration {
	auctionInterval := os.Getenv("AUCTION_INTERVAL")
	duration, err := time.ParseDuration(auctionInterval)
	if err != nil {
		logger.Error("AUCTION_INTERVAL not set correctly; defaulting to 5 minutes.", err)
		return time.Minute * 5
	}

	return duration
}

func calculateAuctionEndTime(auctionEntity auction_entity.Auction) time.Duration {
	auctionEndTime := auctionEntity.Timestamp.Add(getAuctionInterval())
	return time.Until(auctionEndTime)
}

func (ar *AuctionRepository) closeAuction(ctx context.Context, auctionEntity auction_entity.Auction) error {
	filter := bson.M{"_id": auctionEntity.Id}
	update := bson.M{"$set": bson.M{"status": auction_entity.Completed}}

	_, err := ar.Collection.UpdateOne(
		ctx,
		filter,
		update,
		options.Update().SetUpsert(false),
	)

	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Auction %s closed automatically", auctionEntity.Id))

	return nil
}
