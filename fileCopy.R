tickers = c( "AAPL", "GE","INTC","CSCO","YHOO","GOOG","MGM","ORCL","AMZN","EBAY","AMD","DELL","AA","AMGN","GM","IBM","RIMM","HD","MS","QCOM")

startDate = 20070101
endDate = 20071231    # Get all available dates of 2007

fromDrv = "e:"
toDrv = "mac:"

dates = list.files( sprintf( "%s/trades/", fromDrv ) )
dates = as.numeric( dates )
dates = dates[ ! is.na( dates ) ]
goodDates = dates[ which( ( dates >= 20070101 ) & ( dates <= 20071232 ) ) ]

for( date in goodDates ) {

	#
	# Create target trades dir
	#
		dir.create( 
			sprintf( "%s/trades/%d", toDrv, date ), 
			recursive = TRUE 
		)

	#
	# Create target quotes dir
	#
		dir.create( 
			sprintf( "%s/quotes/%d", toDrv, date ), 
			recursive = TRUE 
		)

	#
	# Copy trades
	#
		for( ticker in tickers ) {
			fromFile = sprintf( 
				"%s/trades/%s/%s_trades.binRT",
				fromDrv,
				date,
				ticker
			)
			toFile = sprintf( 
				"%s/trades/%s/%s_trades.binRT",
				toDrv,
				date,
				ticker
			)
			cat( sprintf( "Copying from %s to %s\n", fromFile, toFile ) )
			file.copy( fromFile, toFile, overwrite = FALSE )
		}

	#
	# Copy quotes
	#
		for( ticker in tickers ) {
			fromFile = sprintf( 
				"%s/quotes/%s/%s_quotes.binRQ",
				fromDrv,
				date,
				ticker
			)
			toFile = sprintf( 
				"%s/quotes/%s/%s_quotes.binRQ",
				toDrv,
				date,
				ticker
			)
			cat( sprintf( "Copying from %s to %s\n", fromFile, toFile ) )
			file.copy( fromFile, toFile, overwrite = FALSE )
		}

}