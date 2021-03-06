'''
Extracting program of illegal constructions or buildings from Road Name Address System.

1. Ready raw file from KAIS(https://www.kais.kr/login/login.htm)
1-1. Remove duplicated road name addresses

2. Ready raw file from 새움터
2-1. Get additional RNA(road name addresses) from (juso.go.kr)
2-2. Reomve duplicated road name addresses

* Result *
1 - 2 : Illegal constructions or buildings
2 - 1 : Unreported constructions or buildings or not updated source information
'''

import sys
import pandas as pd
import requests
import geopandas
from shapely.geometry import Point

def extractFromKaisSeum():
	# define directory of raw files
	rawDirKais = "source/kais.xlsx"
	rawDirSeum = "source/seum.xlsx"

	# read raw files and build dataframe from them
	dfKais = pd.read_excel(rawDirKais, sheet_name='Sheet1')
	dfSeum = pd.read_excel(rawDirSeum, sheet_name='인천 부평구')

	# extract unknown addresses (road name addresses == '')
	dfSeum_unknown = dfSeum[(dfSeum['도로명주소'].isnull()) | (dfSeum['도로명주소'] == '')]['대지위치']
	dfSeum_unknown = dfSeum_unknown.drop_duplicates(keep="last").reset_index(drop=True)

	# write file for uploading to (juso.go.kr)
	dfSeum_unknown.to_csv('output/seum_unknown.txt', sep='\n', index=False, encoding='EUC-KR')

	# waiting for input...(directory of download file)
	print("# of dfSeum_unknown: " + str(len(dfSeum_unknown.index)))
	isUnder3000 = False
	if len(dfSeum_unknown.index) > 3000:
		isUnder3000 = False
		print("The number of unknown addresses exceeds 3000.")
	else:
		isUnder3000 = True
		print("The number of unknown addresses is under 3000.")

	print(
		"Upload file './output/seum_unkonwn.txt' to 'juso.go.kr' and download the result file.\nLet me know of the directory of the download file.\n\nDirectory: ")
	rawDirJuso = input()

	# read download file and build dataframe from it
	if isUnder3000:
		dfJuso = pd.read_excel(rawDirJuso, sheet_name='Sheet0', header=[1])
		dfJuso = dfJuso.dropna(subset=['도로명'])['도로명']
		dfJuso = dfJuso.replace(regex=r'\s*\([\W\w][^\(\)]+\)\s*$',
								value='')  # delete sub-informations ex) " (부평동, 피엔케이파크빌)"

		# drop duplicates and concatenates dataframes
		dfKais_dropped = \
		dfKais.dropna(subset=['도로명주소']).drop_duplicates(subset='도로명주소', keep="last").reset_index(drop=True)['도로명주소']
		dfKais_final = '인천광역시 부평구 ' + dfKais_dropped.astype(str)
		dfSeum_dropped = \
		dfSeum.dropna(subset=['도로명주소']).drop_duplicates(subset='도로명주소', keep="last").reset_index(drop=True)['도로명주소']
		dfSeum_final = pd.concat([dfJuso, dfSeum_dropped], ignore_index=True).drop_duplicates(keep="last")

		dfResult1 = dfKais_final[~(dfKais_final.isin(dfSeum_final))]
		dfResult2 = dfSeum_final[~(dfSeum_final.isin(dfKais_final))]

		# show logs
		print("# of addresses from KAIS: " + str(len(dfKais.index)))
		print("# of 'unique' addresses from KAIS: " + str(len(dfKais_final.index)) + "\n")
		print("# of addresses from 새움터: " + str(len(dfSeum.index)))
		print("# of 'unique' addresses from 새움터: " + str(len(dfSeum_final.index)) + "\n")
		print("# of illegal constructions or buildings: " + str(len(dfResult1.index)))
		print("# of not updated addresses: " + str(len(dfResult2.index)))

		# write result txt file
		dfKais_final.to_csv('output/dfKais_final.txt', sep='\n', index=False, encoding='EUC-KR')
		dfSeum_final.to_csv('output/dfSeum_final.txt', sep='\n', index=False, encoding='EUC-KR')
		dfResult1.to_csv('output/dfResult1.txt', sep='\n', index=False, encoding='EUC-KR')
		dfResult2.to_csv('output/dfResult2.txt', sep='\n', index=False, encoding='EUC-KR')

	else:
		### todo
		dfJuso = pd.read_csv(rawDirJuso)
		print(dfJuso)

	return

def geocodeRNA():
	# set things
	cntGeocodedNaver = 0
	cntGeocodedKakao = 0
	cntGeocodedGoogle = 0

	cntGeocoded = 0

	# read data from file
	dfResult1 = pd.read_csv('output/dfResult1.txt', sep=u"\n", header=None, encoding='EUC-KR')
	cntTotal =  dfResult1.count()

	# iterate geocoding
	for index, row in dfResult1.iterrows():
		mAddresses = dfResult1.loc[index, 0]

		##### DaumKakao API
		url = 'https://dapi.kakao.com/v2/local/search/address.json?query=' + mAddresses
		headers = {'content-type': 'application/json', 'Authorization': 'KakaoAK '}
		r = requests.get(url, headers=headers).json()
		if r['documents'] and len(r['documents']) > 0:
			mX = r['documents'][0]['x']
			mY = r['documents'][0]['y']
			dfResult1.loc[index, 'x'] = mX
			dfResult1.loc[index, 'y'] = mY
			cntGeocodedKakao = cntGeocodedKakao + 1
			cntGeocoded = cntGeocoded + 1
			print(str(index) + '\tK\t' + mAddresses + ' geocoded.\t(x: ' + str(mX) + '\ty:' + str(mY) + ')')

		else:
			##### Naver API
			url = 'https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query=' + mAddresses
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'X-NCP-APIGW-API-KEY-ID':'', 'X-NCP-APIGW-API-KEY': ''}
			r = requests.get(url, headers=headers).json()
			if r['addresses'] and len(r['addresses']) > 0:
				mX = r['addresses'][0]['x']
				mY = r['addresses'][0]['y']
				dfResult1.loc[index, 'x'] = mX
				dfResult1.loc[index, 'y'] = mY
				cntGeocodedNaver = cntGeocodedNaver + 1
				cntGeocoded = cntGeocoded + 1
				print(str(index) + '\tN\t' + mAddresses + ' geocoded.\t(x: ' + mX + '\ty:' + mY + ')')

			else:
				##### Google API
				url = 'https://maps.googleapis.com/maps/api/geocode/json?key=&address=' + mAddresses
				r = requests.get(url).json()
				# "status" : "OVER_QUERY_LIMIT"

				if r['status'] == "OK" and r['results'] and len(r['results']) > 0:
					mLocation = r['results'][0]['geometry']['location']
					mX = mLocation['lng']
					mY = mLocation['lat']
					cntGeocodedGoogle = cntGeocodedGoogle + 1
					cntGeocoded = cntGeocoded + 1
					print(str(index) + '\tG\t' + mAddresses + ' geocoded.\t(x: ' + str(mX) + '\ty:' + str(mY) + ')')

				else:
					print(str(index) + '\t\t' + mAddresses + ' geocode FAILED!!!!')

	# log
	print("# of Targeted Addresses: " + str(cntTotal))
	print("# of Geocoded Addresses by Naver: " + str(cntGeocodedNaver) + "(" + str(cntGeocodedNaver/cntTotal) + "% succeeded)")
	print("# of Geocoded Addresses by DaumKakao: " + str(cntGeocodedKakao) + "(" + str(cntGeocodedKakao/cntTotal) + "% succeeded)")
	print("# of Geocoded Addresses by Google: " + str(cntGeocodedGoogle) + "(" + str(cntGeocodedGoogle/cntTotal) + "% succeeded)")
	print("# of Geocoded Addresses: " + str(cntGeocoded) + "(" + str(cntGeocoded/cntTotal) + "% succeeded)")

	# print geocoded data
	dfResult1.to_csv('output/dfResult1_geocoded.csv', sep='\t', index=False, encoding='EUC-KR')

	return

def convertCSVtoSHP():
	# read data from file
	df = pd.read_csv('output/dfResult1_geocoded.csv', sep='\t', encoding='EUC-KR')

	# drop worthless data
	df = df.dropna(subset=['x']).dropna(subset=['y']).drop('0', 1)

	# combine x, y column to a shapely Point() object
	df['geometry'] = df.apply(lambda a: Point((float(a.x), float(a.y))), axis=1)

	# convert the pandas DataFrame into a GeoDataFrame
	df = geopandas.GeoDataFrame(df, geometry='geometry')

	# print the GeoDataFrame into a shapefile
	df.to_file('output/dfResult1_geocoded.shp', driver='ESRI Shapefile')

	return

def main(argv):
	# extractFromKaisSeum()
	# geocodeRNA()
	convertCSVtoSHP()

	return

if __name__ == "__main__":
    main(sys.argv)

