from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import os
import pyodbc

class GdalErrorHandler(object):
    def __init__(self):
        self.err_level=gdal.CE_None
        self.err_no=0
        self.err_msg=''

    def handler(self, err_level, err_no, err_msg):
        self.err_level=err_level
        self.err_no=err_no
        self.err_msg=err_msg

def convertVctToShp(in_fpath, out_fpath=None):
    if not os.path.isfile(in_fpath):
        print('Invalid path of input file!')
        return

    fsplit = os.path.split(in_fpath)
    file = fsplit[-1]
    fname_split = file.split('.')

    if not out_fpath is None:
        out_fsplit = os.path.split(out_fpath)
        if not os.path.isfile(out_fsplit[0]):
            print('Invalid folder for output file!')
            return
        dst_layerName = out_fsplit[-1].split('.')[0]
    else:
        #derive it from in_fpath
        out_dir = fsplit[0] + "/output"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        out_fpath = out_dir + "/" + fname_split[0] + ".shp"
        dst_layerName = fname_split[0]

    err = GdalErrorHandler()
    handler = err.handler
    gdal.PushErrorHandler(handler)
    gdal.UseExceptions()

    try:
        src_ds = ogr.Open(in_fpath)
    except Exception as e:
        print("GDAL doesn't support file {}".format(in_fpath))

    src_driver = src_ds.GetDriver()
    src_layer = src_ds.GetLayer()

    dst_driver = ogr.GetDriverByName("ESRI Shapefile")

    # remove output shapefile if it already exists
    if os.path.exists(out_fpath):
        dst_driver.DeleteDataSource(out_fpath)

    try:
        dst_ds = dst_driver.CreateDataSource(out_fpath)

        # create the spatial reference, WGS84
        src_cs = src_layer.GetSpatialRef()
        if src_cs:
            dst_cs = src_cs
        else:
            dst_cs = osr.SpatialReference()
            dst_cs.ImportFromEPSG(4326)

        src_feature = src_layer.GetNextFeature()
        src_geom = src_feature.GetGeometryRef()
        src_layer.ResetReading()

        # create layer
        geom_types = dict(
            POLYGON = ogr.wkbPolygon
        )
        dst_layer = dst_ds.CreateLayer(dst_layerName, dst_cs, geom_types[src_geom.GetGeometryName()])

        field_types = dict(
            INTEGER = ogr.OFTInteger,
            VARCHAR = ogr.OFTString
        )

        #connect to MS Access DB (where attrbitues are stored)
        acc_file_base = fsplit[0] + '/' + fname_split[0]
        acc_file = acc_file_base + '.accdb'
        if not os.path.isfile(acc_file):
            acc_file = acc_file_base + '.mdb'

        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + acc_file + ';'
        )
        try:
            cnxn = pyodbc.connect(conn_str)
        except:
            print('Program couldn\'t connect to MS Access')
            return

        crsr = cnxn.cursor()
        acc_found = False
        for table_info in crsr.tables(tableType='TABLE'):
            if table_info.table_name == fname_split[0]:
                acc_table = fname_split[0]
                acc_found = True
                break
        if not acc_found:
            print('Related attribute table ', acc_table, ' not found in MS Access! ')
            return

        field_names = dict()
        pk = None
        for column in crsr.columns(table=acc_table):
            if pk is None:
                pk = column.column_name

            new_field_name = column.column_name
            # try to get valuable info from warning message
            try:
                dst_layer.CreateField(ogr.FieldDefn(column.column_name, field_types[column.type_name]))
            except Exception as e:
                pass
            else:
                if err.err_level >= gdal.CE_Warning:
                    new_field_name = err.err_msg.split()[-1].replace('\'', '')

                    # i'm trying to stop the warning to be caught again. Is there a better way?
                    err.err_level = gdal.CE_None

            field_names[column.column_name]=new_field_name

        # look for real PK, if not found then first field is considered a PK
        for stats in crsr.statistics(table=acc_table):
            if "PrimaryKey" in stats:
                for stat in stats:
                    if stat in field_names.keys():
                        pk = stat
                        break

        dst_layerDefn = dst_layer.GetLayerDefn()

        # loop over features
        for src_feature in src_layer:
            src_fid = int(src_feature.items()[src_feature.keys()[0]])

            crsr.execute("select * from " + acc_table + " where " + pk + "=" + str(src_fid))
            row = crsr.fetchone()

            src_geom = src_feature.GetGeometryRef()
            dst_feature = ogr.Feature(dst_layerDefn)
            dst_feature.SetGeometry(src_geom.Clone())

            for fn, new_fn in field_names.items():
                fval = getattr(row, fn)
                dst_feature.SetField(new_fn, fval)

            dst_layer.CreateFeature(dst_feature)
            dst_feature = None

    except IOError as e:
        print("Conversion to SHP failed due to {}.".forma(e))

    src_ds = None
    dst_ds = None
    print("Translation successful! See {}".format(out_fpath))

convertVctToShp('c:/temp/vct/world_nations.vct')
