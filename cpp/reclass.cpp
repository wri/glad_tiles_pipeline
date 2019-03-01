#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <sys/stat.h>
#include <math.h>
#include <algorithm>
#include <string.h>
#include <stdint.h>
#include <sstream>
#include <iomanip>
#include <gdal_priv.h>
#include <cpl_conv.h>
#include <ogr_spatialref.h>
using namespace std;

int main(int argc, char* argv[])
{

// code to write out a binary values | no values raster
// the value written if we have data is the reclass_output_val
// useful when we need to prep an intensity raster, and want all our
// pixels to be 1 at our native zoom level, before resampling up to lower zooms

//passing arguments
if (argc != 4){cout << "Use <program name> <input name> <output name> <reclass_output_val>" << endl; return 1;}
string in_name=argv[1];
string out_name=argv[2];
uint8_t day_offset=atoi(argv[3]);

//setting variables
int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL; GDALRasterBand  *INBAND;

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(in_name.c_str(), GA_ReadOnly );
INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); ysize=INBAND->GetYSize();
INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; uly=GeoTransform[3]; pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALRasterBand *OUTBAND;
OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "DEFLATE" );
papszOptions = CSLSetNameValue( papszOptions, "TILED", "YES" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };
OUTGDAL = OUTDRIVER->Create( out_name.c_str(), xsize, ysize, 1, GDT_Byte, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform);
OUTGDAL->SetProjection(OUTPRJ);
OUTBAND = OUTGDAL->GetRasterBand(1);
OUTBAND->DeleteNoDataValue();

//read/write data
uint16_t in_data[xsize];
uint8_t out_data[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, in_data, xsize, 1, GDT_UInt16, 0, 0);
for(x=0; x<xsize; x++) {
if (in_data[x]>0){out_data[x]=day_offset;}
else {out_data[x]=0;}
}
OUTBAND->RasterIO( GF_Write, 0, y, xsize, 1, out_data, xsize, 1, GDT_Byte, 0, 0 );
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}