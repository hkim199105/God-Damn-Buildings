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
from geopandas import GeoDataFrame
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
	dfResult1 = pd.read_csv('output/dfResult1.txt', sep=u"\n", header=None, encoding='EUC-KR')
	for index, row in dfResult1.iterrows():
		mAddresses = dfResult1.loc[index, 0]
		url = 'https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query=' + mAddresses
		headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'X-NCP-APIGW-API-KEY-ID':'x', 'X-NCP-APIGW-API-KEY': 'x'}
		r = requests.get(url, headers=headers).json()
		if r['addresses'] and len(r['addresses']) > 0:
			mX = r['addresses'][0]['x']
			mY = r['addresses'][0]['y']
			dfResult1.loc[index, 'x'] = mX
			dfResult1.loc[index, 'y'] = mY
			print(mAddresses + ' geocoded.\t(x: ' + mX + '\ty:' + mY + ')')
		else:
			print(mAddresses + ' geocode failed.')


	dfResult1.to_csv('output/dfResult1_geocoded.csv', sep='\n', index=False, encoding='EUC-KR')

	return

def convertCSVtoSHP():
	dfTemp = pd.read_csv('output/dfResult1_geocoded.csv', sep=u"\n", header=None, encoding='EUC-KR')

	geometry = [Point(xy) for xy in zip(dfTemp.x, dfTemp.y)]
	crs = {'init': 'epsg:4326'}
	geo_df = GeoDataFrame(dfTemp, crs=crs, geometry=geometry)
	geo_df.to_file(driver='ESRI Shapefile', filename='output/Result_geocoded.shp')

	return

def main(argv):
	# extractFromKaisSeum()
	# geocodeRNA()
	convertCSVtoSHP()

	return

if __name__ == "__main__":
    main(sys.argv)

