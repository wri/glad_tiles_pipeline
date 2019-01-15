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
#include <gdal/gdal_priv.h>
#include <gdal/cpl_conv.h>
#include <gdal/ogr_spatialref.h>
using namespace std;

int main(int argc, char* argv[])
{

// Code to combine three input rasters by checking if the first has data, and
// if so writing it, otherwise checking the second and writing it if it has
// data, otherwise using the value from the third.
// Shouldn't be necessary given our assumptions about GLAD alerts (there can't
// be an alert twice in the same place) but we've often seen this in the data

//passing arguments
if (argc != 5){cout << "Use <program name> <input1 name> <input2 name> <input3 name> <output name>" << endl; return 1;}
string in1_name=argv[1];
string in2_name=argv[2];
string in3_name=argv[3];
string out_name=argv[4];

//setting variables
int x, y;
int xsize, ysize;
double GeoTransform[6]; double ulx, uly; double pixelsize; double NoData1, NoData2, NoData3;

//initialize GDAL for reading
GDALAllRegister();
GDALDataset  *INGDAL; GDALRasterBand  *INBAND;
GDALDataset  *INGDAL2; GDALRasterBand  *INBAND2;
GDALDataset  *INGDAL3; GDALRasterBand  *INBAND3;

//open file and get extent and projection
INGDAL = (GDALDataset *) GDALOpen(in1_name.c_str(), GA_ReadOnly ); INBAND = INGDAL->GetRasterBand(1);
xsize=INBAND->GetXSize(); ysize=INBAND->GetYSize();

INGDAL->GetGeoTransform(GeoTransform);
ulx=GeoTransform[0]; uly=GeoTransform[3]; pixelsize=GeoTransform[1];
cout << xsize <<", "<< ysize <<", "<< ulx <<", "<< uly << ", "<< pixelsize << endl;
INGDAL2 = (GDALDataset *) GDALOpen(in2_name.c_str(), GA_ReadOnly ); INBAND2 = INGDAL2->GetRasterBand(1);
INGDAL3 = (GDALDataset *) GDALOpen(in3_name.c_str(), GA_ReadOnly ); INBAND3 = INGDAL3->GetRasterBand(1);

NoData1 = INBAND->GetNoDataValue();
NoData2 = INBAND2->GetNoDataValue();
NoData3 = INBAND3->GetNoDataValue();

if (( NoData1 != NoData2) || ( NoData2 != NoData3 )) {
   cout << "Input NoData values don't match" << endl; exit ( 1 );
   };

//initialize GDAL for writing
GDALDriver *OUTDRIVER;
GDALDataset *OUTGDAL;
GDALRasterBand *OUTBAND;
OGRSpatialReference oSRS;
char *OUTPRJ = NULL;
char **papszOptions = NULL;
papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "DEFLATE" );
papszOptions = CSLSetNameValue( papszOptions, "TILEd", "YES" );
OUTDRIVER = GetGDALDriverManager()->GetDriverByName("GTIFF"); if( OUTDRIVER == NULL ) {cout << "no driver" << endl; exit( 1 );};
oSRS.SetWellKnownGeogCS( "WGS84" );
oSRS.exportToWkt( &OUTPRJ );
double adfGeoTransform[6] = { ulx, pixelsize, 0, uly, 0, -1*pixelsize };
OUTGDAL = OUTDRIVER->Create( out_name.c_str(), xsize, ysize, 1, GDT_UInt16, papszOptions );
OUTGDAL->SetGeoTransform(adfGeoTransform); OUTGDAL->SetProjection(OUTPRJ); OUTBAND = OUTGDAL->GetRasterBand(1);

if ( NoData1 != -10000000000 ) {
OUTBAND->SetNoDataValue(NoData1);
} else {
cout << "No Data value not present in input rasters, not setting in output" << endl;
}

//read/write data
uint16_t in1_data[xsize];
uint16_t in2_data[xsize];
uint16_t in3_data[xsize];
unsigned short out_data[xsize];

for(y=0; y<ysize; y++) {
INBAND->RasterIO(GF_Read, 0, y, xsize, 1, in1_data, xsize, 1, GDT_UInt16, 0, 0);
INBAND2->RasterIO(GF_Read, 0, y, xsize, 1, in2_data, xsize, 1, GDT_UInt16, 0, 0);
INBAND3->RasterIO(GF_Read, 0, y, xsize, 1, in3_data, xsize, 1, GDT_UInt16, 0, 0);
for(x=0; x<xsize; x++) {
  if(in1_data[x] > 0) {
     out_data[x] = in1_data[x];
     }
  else {
    if (in2_data[x] > 0) {
        out_data[x] = in2_data[x];
         }
    else {
        out_data[x] = in3_data[x];
         }
       }
}
OUTBAND->RasterIO( GF_Write, 0, y, xsize, 1, out_data, xsize, 1, GDT_UInt16, 0, 0 );
}

//close GDAL
GDALClose(INGDAL);
GDALClose((GDALDatasetH)OUTGDAL);

return 0;
}