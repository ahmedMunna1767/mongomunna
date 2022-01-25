package mongomunna

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Close resources taken by the database connection
func Close(client *mongo.Client, ctx context.Context, cancel context.CancelFunc) {
	defer cancel()

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
}

//Connect to the database
func Connect(uri string) (*mongo.Client, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	return client, ctx, cancel, err
}

// Insert one item at a time. input binary json doc
func InsertOne(client *mongo.Client, ctx context.Context, database, col string, data interface{}) (*mongo.InsertOneResult, error) {
	collection := client.Database(database).Collection(col)
	result, err := collection.InsertOne(ctx, data)
	return result, err
}

// Insert a doc array at a time. input binary json doc array
func InsertMany(client *mongo.Client, ctx context.Context, database, col string, data []interface{}) (*mongo.InsertManyResult, error) {
	collection := client.Database(database).Collection(col)
	result, err := collection.InsertMany(ctx, data)
	return result, err
}

// Query to retrieve data. returns a cursor
func Find(client *mongo.Client, ctx context.Context, dataBase, col string, query, field interface{}) (*mongo.Cursor, error) {
	collection := client.Database(dataBase).Collection(col)
	result, err := collection.Find(ctx, query, options.Find().SetProjection(field))
	return result, err
}

// Update the first data object
func UpdateOne(client *mongo.Client, ctx context.Context, dataBase, col string, filter, update interface{}) (*mongo.UpdateResult, error) {
	collection := client.Database(dataBase).Collection(col)
	result, err := collection.UpdateOne(ctx, filter, update)
	return result, err
}

// Update all the objects matching the filter
func UpdateMany(client *mongo.Client, ctx context.Context, dataBase, col string, filter, update interface{}) (*mongo.UpdateResult, error) {
	collection := client.Database(dataBase).Collection(col)
	result, err := collection.UpdateMany(ctx, filter, update)
	return result, err
}

// Delete the first data object
func DeleteOne(client *mongo.Client, ctx context.Context, dataBase, col string, query interface{}) (*mongo.DeleteResult, error) {
	collection := client.Database(dataBase).Collection(col)
	result, err := collection.DeleteOne(ctx, query)
	return result, err
}

// Delete all the objects matching the filter
func DeleteMany(client *mongo.Client, ctx context.Context, dataBase, col string, query interface{}) (*mongo.DeleteResult, error) {
	collection := client.Database(dataBase).Collection(col)
	result, err := collection.DeleteMany(ctx, query)
	return result, err
}
