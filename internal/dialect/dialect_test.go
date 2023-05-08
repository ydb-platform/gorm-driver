package dialect

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm/schema"
)

type Season struct {
	ID         string    `gorm:"column:season_id;primarykey"`
	SeriesID   string    `gorm:"column:series_id;index"`
	Title      string    `gorm:"column:title;not null"`
	FirstAired time.Time `gorm:"column:first_aired;not null"`
	LastAired  time.Time `gorm:"column:last_aired;not null"`
}

type Episode struct {
	ID       string    `gorm:"column:episode_id;primarykey"`
	SeasonID string    `gorm:"column:season_id;index;not null"`
	Title    string    `gorm:"column:title;not null"`
	AirDate  time.Time `gorm:"column:air_date;not null"`
}

type Series struct {
	ID          string    `gorm:"column:series_id;primarykey;not null"`
	Title       string    `gorm:"column:title;not null"`
	Info        string    `gorm:"column:series_info"`
	Comment     string    `gorm:"column:comment"`
	ReleaseDate time.Time `gorm:"column:release_date;not null"`
}

func Test_createTableQuery(t *testing.T) {
	d := ydbDialect{tablePathPrefix: "/testing/mock"}
	for _, tt := range []struct {
		model interface{}
		sql   string
	}{
		{
			model: Episode{},
			sql: `
CREATE TABLE "/testing/mock/episodes" (
	"episode_id" Utf8, 
	"season_id" Utf8, 
	"title" Utf8, 
	"air_date" Datetime,
	PRIMARY KEY ("episode_id"),
	INDEX "idx_episodes_season_id" GLOBAL ON ("season_id")
);`,
		},
		{
			model: Season{},
			sql: `
CREATE TABLE "/testing/mock/seasons" (
	"season_id" Utf8, 
	"series_id" Utf8, 
	"title" Utf8, 
	"first_aired" Datetime, 
	"last_aired" Datetime,
	PRIMARY KEY ("season_id"),
	INDEX "idx_seasons_series_id" GLOBAL ON ("series_id")
);`,
		},
		{
			model: Series{},
			sql: `
CREATE TABLE "/testing/mock/series" (
	"series_id" Utf8, 
	"title" Utf8, 
	"series_info" Utf8, 
	"comment" Utf8, 
	"release_date" Datetime,
	PRIMARY KEY ("series_id")
);`,
		},
	} {
		t.Run("", func(t *testing.T) {
			sql, err := d.createTableQuery(tt.model)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(strings.TrimSpace(tt.sql), "\"", "`"), sql)
		})
	}
}

func Test_dropTableQuery(t *testing.T) {
	d := ydbDialect{tablePathPrefix: "/testing/mock"}
	for _, tt := range []struct {
		model interface{}
		sql   string
	}{
		{
			model: Episode{},
			sql:   `DROP TABLE "/testing/mock/episodes";`,
		},
		{
			model: Season{},
			sql:   `DROP TABLE "/testing/mock/seasons";`,
		},
		{
			model: Series{},
			sql:   `DROP TABLE "/testing/mock/series";`,
		},
	} {
		t.Run("", func(t *testing.T) {
			sql, err := d.dropTableQuery(tt.model)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(strings.TrimSpace(tt.sql), "\"", "`"), sql)
		})
	}
}

func Test_addColumnQuery(t *testing.T) {
	d := ydbDialect{tablePathPrefix: "/testing/mock"}
	for _, tt := range []struct {
		model      interface{}
		columnName string
		sql        string
	}{
		{
			model:      Episode{},
			columnName: "air_date",
			sql:        `ALTER TABLE "episodes" ADD COLUMN "air_date" Datetime;`,
		},
		{
			model:      Season{},
			columnName: "title",
			sql:        `ALTER TABLE "seasons" ADD COLUMN "title" Utf8;`,
		},
		{
			model:      Series{},
			columnName: "comment",
			sql:        `ALTER TABLE "series" ADD COLUMN "comment" Utf8;`,
		},
	} {
		t.Run("", func(t *testing.T) {
			sql, err := d.addColumnQuery(tt.model, tt.columnName)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(strings.TrimSpace(tt.sql), "\"", "`"), sql)
		})
	}
}

func Test_dropIndexQuery(t *testing.T) {
	d := ydbDialect{tablePathPrefix: "/testing/mock"}
	for _, tt := range []struct {
		model      interface{}
		columnName string
		sql        string
	}{
		{
			model:      Episode{},
			columnName: "air_date",
			sql:        `ALTER TABLE "/testing/mock/episodes" DROP COLUMN "air_date";`,
		},
		{
			model:      Season{},
			columnName: "title",
			sql:        `ALTER TABLE "/testing/mock/seasons" DROP COLUMN "title";`,
		},
		{
			model:      Series{},
			columnName: "comment",
			sql:        `ALTER TABLE "/testing/mock/series" DROP COLUMN "comment";`,
		},
	} {
		t.Run("", func(t *testing.T) {
			sql, err := d.dropColumnQuery(tt.model, tt.columnName)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(strings.TrimSpace(tt.sql), "\"", "`"), sql)
		})
	}
}

func Test_createIndexQuery(t *testing.T) {
	d := ydbDialect{tablePathPrefix: "/testing/mock"}
	for _, tt := range []struct {
		tableName string
		index     *schema.Index
		sql       string
	}{
		{
			tableName: "episodes",
			index: &schema.Index{
				Name: "idx_episodes_title",
				Fields: []schema.IndexOption{
					{
						Field: &schema.Field{
							DBName: "title",
						},
					},
				},
			},
			sql: `ALTER TABLE "/testing/mock/episodes" ADD INDEX "idx_episodes_title" GLOBAL ON ("title");`,
		},
		{
			tableName: "seasons",
			index: &schema.Index{
				Name: "idx_seasons_title_first_aired_last_aired",
				Fields: []schema.IndexOption{
					{
						Field: &schema.Field{
							DBName: "title",
						},
					},
					{
						Field: &schema.Field{
							DBName: "first_aired",
						},
					},
					{
						Field: &schema.Field{
							DBName: "last_aired",
						},
					},
				},
			},
			sql: `ALTER TABLE "/testing/mock/seasons" ` +
				`ADD INDEX "idx_seasons_title_first_aired_last_aired" ` +
				`GLOBAL ON ("title", "first_aired", "last_aired");`,
		},
		{
			tableName: "series",
			index: &schema.Index{
				Name: "idx_series_title_info",
				Fields: []schema.IndexOption{
					{
						Field: &schema.Field{
							DBName: "title",
						},
					},
					{
						Field: &schema.Field{
							DBName: "info",
						},
					},
				},
			},
			sql: `ALTER TABLE "/testing/mock/series" ADD INDEX "idx_series_title_info" GLOBAL ON ("title", "info");`,
		},
	} {
		t.Run("", func(t *testing.T) {
			sql, err := d.createIndexQuery(tt.tableName, tt.index)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(strings.TrimSpace(tt.sql), "\"", "`"), sql)
		})
	}
}

func Test_dropColumnQuery(t *testing.T) {
	d := ydbDialect{tablePathPrefix: "/testing/mock"}
	for _, tt := range []struct {
		model     interface{}
		indexName string
		sql       string
	}{
		{
			model:     Episode{},
			indexName: "idx_episodes_title",
			sql:       `ALTER TABLE "/testing/mock/episodes" DROP COLUMN "idx_episodes_title";`,
		},
		{
			model:     Season{},
			indexName: "idx_seasons_title_first_aired_last_aired",
			sql:       `ALTER TABLE "/testing/mock/seasons" DROP COLUMN "idx_seasons_title_first_aired_last_aired";`,
		},
		{
			model:     Series{},
			indexName: "idx_series_title_info",
			sql:       `ALTER TABLE "/testing/mock/series" DROP COLUMN "idx_series_title_info";`,
		},
	} {
		t.Run("", func(t *testing.T) {
			sql, err := d.dropColumnQuery(tt.model, tt.indexName)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(strings.TrimSpace(tt.sql), "\"", "`"), sql)
		})
	}
}
