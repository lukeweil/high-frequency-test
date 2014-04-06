#
#Imbalance function
#
vwap = function(
	trades,		# In format trades$r[ , <type> ] 
			# where <type> is c( "m", "s", "p" )
	endTimeOffst	# Millisecond offset from midnight
) {
	k = which( trades$r[ , "m" ] <= endTimeOffst )
	sum( trades$r[ k, "s" ] * trades$r[ k, "p" ] ) / sum( trades$r[ k, "s" ] )
}


#
# Tick test
#
	tickTest = function ( prices ) {
		n = length ( prices )
		signs = c ( 0 , sign ( diff ( prices ) ) )
		n = length ( signs )
		type = integer ( n )
		t = 0 
		for ( i in 1 : n ) {
			if ( signs [ i ] != 0 ) {
				if ( t != signs [ i ] ) {
					t = signs [ i ] 
				}
			}
			type [ i ] = t
		}
		type
	}

#
# Compute imbalance to a given time offset in the trading day
#
	imbalance = function( 
		trades,		# In format trades$r[ , <type> ] 
				# where <type> is c( "m", "s", "p" )
		endTimeOffst	# Millisecond offset from midnight
	) {
		a = cumsum( tickTest( trades$r[ , "p" ] ) * trades$r[ , "s" ] )
		a[ which( trades$r[ , "m" ] <= endTimeOffst ) ]
	}


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
		dvMat = matrix( ncol= length( dates ) , nrow = length( tickers ) )
		mdvMat = matrix( ncol= length( dates ) , nrow = length( tickers ) )
		X = matrix( ncol= length( dates ) , nrow = length( tickers ) )
		S400=matrix( ncol= length( dates ) , nrow = length( tickers ) )
		S930=matrix( ncol= length( dates ) , nrow = length( tickers ) )
		VWAP=matrix( ncol= length( dates ) , nrow = length( tickers ) )
		sigma=matrix ( ncol = length( dates ), nrow = length( tickers ) )
		for( i in 1:nStocks ) {
			for( j in 1:length( dates ) ) {

				date = dates[ j ]
				file = sprintf( "%s/quotes/%d/%s_quotes.binRQ", baseDir, date, tickers[ i ] )

				#
				# Indices into results matrix
				#
					from = 1 + ( ( j - 1 ) * numBuckets ) 
					to = j * numBuckets
					fromBack = 1 + ( ( j - 10 ) * numBuckets )

				#
				# Report which file we're working with
				#
					if( verbose ) {
						cat( file, "\n" )
					}

				stkQ = tryCatch (
					readBinRQ( file ),
					error = function ( e ) { NULL },
					warning = function ( w ) { NULL }
				)

				if ( is.null( stkQ ) ) {

					#
					# Failed to read file - Fill with NAs
					#
						retMat[ i, from:to ] = rep( NA, numBuckets )

				} else {
					

					#
					# File read successfully - Fill relevant part of results matrix with actual data
					#
						bucketNum = floor( numBuckets * (( stkQ$r[ , "m" ] - startOfDay ) / lenOfDay) ) + 1
						w=dim(bucketNum)
						cat(w)
						midQuotes = ( stkQ$r[ , "bp" ] + stkQ$r[ , "ap" ] ) / 2
						priceBuckets = matrix( ncol =1 , nrow = numBuckets)
						priceBuckets[ bucketNum ] = midQuotes
						if ( is.na(priceBuckets[1])){
							priceBuckets[1]=midQuotes[ 1 ]
							}
						for ( k in 2:numBuckets ) {
							if ( is.na(priceBuckets[k])){
								priceBuckets[k]=priceBuckets[k-1]
								}
							}
						retMat[ i, from:to ] = ( priceBuckets / c( midQuotes[ 1 ], priceBuckets[ 1 : ( numBuckets - 1 ) ] ) ) - 1					#Read the trades
					trades = readBinRT(file)
					#Calculate the daily volume
					dvMat[i,j] = sum(trades$r[ , "s" ])
					#Calculate the initial prices
					S930[i,j]=midQuotes[2]
					#Process transactions 3:30-6:30
					W=imbalance(trades,31 * 60 * 60 * 1000 / 2)
					X[i,j]=W[length(W)]
					VWAP[i,j]=vwap(trades,31 * 60 * 60 * 1000 / 2)
					#save terminal price at 4:00
					S400[i,j]=( stkQ$r[nrow(stkQ$r)-1 , "bp" ] + stkQ$r[nrow(stkQ$r)-1, "ap" ] ) / 2
					#Calculate the standard deviation if possible
					if ( j > 9 ){
						sigma[i,j] = var(retMat[ i, fromBack:to])
						mdvMat[i,j] = mean(dvMat[i,j-9:j])
						}
				}
			}
		}
		rownames( retMat ) = tickers
		rownames( dvMat ) = tickers
		result = list();
		result[["returns"]]=retMat;
		result[["volume"]]=dvMat;
		result[["meanVolume"]]=mdvMat;
		result[["S930"]]=S930;
		result[["S400"]]=S400;
		result[["VWAP"]]=VWAP;
		result[["X"]]=X;
		result[["variance"]]=sigma;
		result
	} # end of getRetMat(...)

quoteDates = function( baseDir, startDate, endDate ) {
	dates = list.files( sprintf( "%s/quotes", baseDir ) )
	dates = as.numeric( dates )
	dates = dates[ ! is.na( dates ) ]
	dates[ which( ( dates >= startDate ) & ( dates <= endDate ) & (dates != 20070703) ) ]
}

baseDir = "c"
tickers=c("AAPL","GE","INTC","CSCO","YHOO","GOOG","MGM","ORCL","AMZN","EBAY","AMD","DELL","AA","AMGN","GM","IBM","RIMM","HD","MS","QCOM")
dates = quoteDates( baseDir, 20070101, 20071232 )

#
# Principal loop
#
	numBuckets = 195	# 6.5 hours = 195 x 2 min
	naive20 = getRetMat(
		baseDir,	# base dir where subdir 'quotes' resides
		tickers,	# list of tickers, e.g. c( "IBM", "DELL", "MSFT" ) 
		dates, 		# list of dates, inclusive, e.g. c( 20070620, 20070622 )
		numBuckets 	# number of return buckets to produce, e.g. 26 for 15 min returns
	)

I=(naive20$S400-naive20$S930)/naive20$S930
a=dim(I)
final=a[2];
J=(naive20$VWAP-naive20$S930)/naive20$S930
h=J[,10:final]-I[,10:final]/2						#Temporary Impact
sigma=sqrt(195*naive20$variance[,10:final])
volume=naive20$meanVolume[,10:final]
X=naive20$X[,10:final]
dim(h)<-c(a[1]*(a[2]-9),1)
dim(sigma)<-c(a[1]*(a[2]-9),1)
dim(volume)<-c(a[1]*(a[2]-9),1)
dim(X)<-c(a[1]*(a[2]-9),1)
reg <- nls(h ~ sigma*eta*sign(X)*(6.5*abs(X)/(6*volume)^beta),start=list(eta = 1, beta = .5),trace = T)








