package common

import (
	"context"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type AssetType string

const (
	CommonStock AssetType = "Common Stock"
	ETF         AssetType = "Exchange Traded Fund"
	ETN         AssetType = "Exchange Traded Note"
	Fund        AssetType = "Closed-End Fund"
	MutualFund  AssetType = "Mutual Fund"
	ADRC        AssetType = "American Depository Receipt Common"
)

type Asset struct {
	Ticker               string    `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name                 string    `json:"Name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Description          string    `json:"description" parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PrimaryExchange      string    `json:"primary_exchange" parquet:"name=primary_exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType            AssetType `json:"asset_type" parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi        string    `json:"composite_figi" parquet:"name=composite_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ShareClassFigi       string    `json:"share_class_figi" parquet:"name=share_class_figi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CUSIP                string    `json:"cusip" parquet:"name=cusip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ISIN                 string    `json:"isin" parquet:"name=isin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CIK                  string    `json:"cik" parquet:"name=cik, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ListingDate          string    `json:"listing_date" parquet:"name=listing_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DelistingDate        string    `json:"delisting_date" parquet:"name=delisting_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Industry             string    `json:"industry" parquet:"name=industry, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sector               string    `json:"sector" parquet:"name=sector, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Icon                 []byte    `json:"icon"`
	IconUrl              string    `json:"icon_url" parquet:"name=icon_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CorporateUrl         string    `json:"corporate_url" parquet:"name=corporate_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HeadquartersLocation string    `json:"headquarters_location" parquet:"name=headquarters_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SimilarTickers       []string  `json:"similar_tickers" parquet:"name=similar_tickers, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	PolygonDetailAge     int64     `json:"polygon_detail_age" parquet:"name=polygon_detail_age, type=INT64"`
	LastUpdated          int64     `json:"last_updated" parquet:"name=last_update, type=INT64"`
	Source               string    `json:"source" parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func ReadAssetsFromDatabase(assetTypes []string) []*Asset {
	log.Info().Msg("reading from database")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return []*Asset{}
	}
	defer conn.Close(ctx)

	var assets []*Asset
	pgxscan.Select(ctx, conn, &assets, `SELECT ticker, name, asset_type, composite_figi FROM assets WHERE active='t' and asset_type = any($1)`, assetTypes)
	return assets
}

func (asset *Asset) MarshalZerologObject(e *zerolog.Event) {
	e.Str("Ticker", asset.Ticker)
	e.Str("Name", asset.Name)
	e.Str("Description", asset.Description)
	e.Str("PrimaryExchange", asset.PrimaryExchange)
	e.Str("AssetType", string(asset.AssetType))
	e.Str("CompositeFigi", asset.CompositeFigi)
	e.Str("ShareClassFigi", asset.ShareClassFigi)
	e.Str("CUSIP", asset.CUSIP)
	e.Str("ISIN", asset.ISIN)
	e.Str("CIK", asset.CIK)
	e.Str("ListingDate", asset.ListingDate)
	e.Str("DelistingDate", asset.DelistingDate)
	e.Str("Industry", asset.Industry)
	e.Str("Sector", asset.Sector)
	e.Str("IconUrl", asset.IconUrl)
	e.Str("CorporateUrl", asset.CorporateUrl)
	e.Str("HeadquartersLocation", asset.HeadquartersLocation)
	e.Str("Source", asset.Source)
	e.Int64("PolygonDetailAge", asset.PolygonDetailAge)
	e.Int64("LastUpdate", asset.LastUpdated)
}
