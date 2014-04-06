# --------------------------------------------------------------------------------------------
# Retrieves day of trade data in format
# h => list ( n => <numRecords>, s => <secondsFromEpoch> )
# r => matrix (
#   m => <millisFromMid>,
#   bs => <bidSize>,
#   bp => <bidPrice>,
#   as => <askSize>,
#   ap => <askPrice>
# )
# --------------------------------------------------------------------------------------------

	readBinRQ = function ( filePathName ) {
		inFile = gzfile ( filePathName, "rb" )
		data = list();
		data [["h"]] = list();
		header = readBin ( inFile, integer(), n = 2, size = 4, endian = "swap" )
		data [["h"]][["s"]] = header [ 1 ]
		data [["h"]][["n"]] = header [ 2 ]
		data [["r"]] = matrix ( nrow = header[2], ncol = 5 )
		colnames(data[["r"]]) = c ( "m", "bs", "bp", "as", "ap" )
		data[["r"]] [,1] = readBin ( inFile, integer(), n = header[2], size = 4, endian = "swap" )
		data[["r"]] [,2] = readBin ( inFile, integer(), n = header[2], size = 4, endian = "swap" )
		data[["r"]] [,3] = readBin ( inFile, numeric(), n = header[2], size = 4, endian = "swap" )
		data[["r"]] [,4] = readBin ( inFile, integer(), n = header[2], size = 4, endian = "swap" )
		data[["r"]] [,5] = readBin ( inFile, numeric(), n = header[2], size = 4, endian = "swap" )
		close ( inFile )
		data
	}

# --------------------------------------------------------------------------------------------
# Retrieves day of trade data in format
# h => list ( n => <numRecords>, s => <secondsFromEpoch> )
# r => matrix ( m => <millisFromMid>, s => <size>, p => <price> )
# --------------------------------------------------------------------------------------------

	readBinRT = function ( filePathName ) {
		inFile = gzfile ( filePathName, "rb" )
		data = list();
		data [["h"]] = list();
		header = readBin ( inFile, integer(), n = 2, size = 4, endian = "swap" )
		data [["h"]][["s"]] = header [ 1 ]
		data [["h"]][["n"]] = header [ 2 ]
		data [["r"]] = matrix ( nrow = header[2], ncol = 3 )
		colnames(data[["r"]]) = c ( "m", "s", "p" )
		data[["r"]] [,1] = readBin ( inFile, integer(), n = header[2], size = 4, endian = "swap" )
		data[["r"]] [,2] = readBin ( inFile, integer(), n = header[2], size = 4, endian = "swap" )
		data[["r"]] [,3] = readBin ( inFile, numeric(), n = header[2], size = 4, endian = "swap" )
		close ( inFile )
		data
	}
#
# Return a matrix where rows are stocks and columns are buckets of returns
#
	getRetMat = function( 

		baseDir,				# base dir where subdir 'quotes' resides
		tickers,				# list of tickers, e.g. c( "IBM", "DELL", "MSFT" ) 
		dates, 					# list of dates, inclusive, e.g. c( 20070620, 20070622 )
		numBuckets, 				# number of return buckets to produce, e.g. 26 for 15 min returns
		startOfDay = 19 * 60 * 60 * 1000 / 2,	# start of trading day in millis from midnight
		lenOfDay = 13 * 60 * 60 * 1000 / 2,	# length of trading day in millis
		verbose = TRUE 				# report which file is being processed

	) {
		nStocks = length( tickers )
		retMat = matrix( ncol = length( dates ) * numBuckets, nrow = length( tickers ) )
		for( i in 1:nStocks ) {
			for( j in 1:length( dates ) ) {

				date = dates[ j ]
				file = sprintf( "%s/quotes/%d/%s_quotes.binRQ", baseDir, date, tickers[ i ] )

				#
				# Indices into results matrix
				#
					from = 1 + ( ( j - 1 ) * numBuckets ) 
					to = j * numBuckets

				#
				# Report which file we're working with
				#
				
						bucketNum = floor( numBuckets * ( stkQ$r[ , "m" ] - startOfDay ) / lenOfDay ) + 1
						midQuotes = ( stkQ$r[ , "bp" ] + stkQ$r[ , "ap" ] ) / 2
						priceBuckets = rep( NA, numBuckets )
						priceBuckets[ bucketNum ] = midQuotes
						retMat[ i, from:to ] = ( priceBuckets / c( midQuotes[ 1 ], priceBuckets[ -length( priceBuckets ) ] ) ) - 1.0
			}
		}
		rownames( retMat ) = tickers
		retMat
	} # end of getRetMat(...)

quoteDates = function( baseDir, startDate, endDate ) {
	dates = list.files( sprintf( "%s/quotes", baseDir ) )
	dates = as.numeric( dates )
	dates = dates[ ! is.na( dates ) ]
	dates[ which( ( dates >= startDate ) & ( dates <= endDate ) ) ]
}

baseDir = "c"
tickers = c("AAPL","IBM")
dates = quoteDates( baseDir, 20070620, 20070624)

#
# Retrieve 15 minute returns for all 20 stocks at once
#
	numBuckets = 26		# 6.5 hours = 26 x 15 min
	naive = getRetMat(
		baseDir,	# base dir where subdir 'quotes' resides
		tickers,	# list of tickers, e.g. c( "IBM", "DELL", "MSFT" ) 
		dates, 		# list of dates, inclusive, e.g. c( 20070620, 20070622 )
		numBuckets 	# number of return buckets to produce, e.g. 26 for 15 min returns
	)

