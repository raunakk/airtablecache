# %%
import airtablecache.airtablecache as AC
import pandas as pd
import os
import sys

testDataLoc = os.path.join(os.path.dirname(sys.modules['airtablecache'].__file__), 'data/testData.csv')

sampleDF = pd.DataFrame([
    ['Alpha', 10, 'India'],
    ['Beta', 15, 'Australia']
], columns=['Name', 'Age', 'Country'])

newDF = pd.DataFrame([
    ['Gamma', 20, 'Canada'],
    ['Beta', 12, 'Australia'],
    ['Alpha', 10, 'India']
], columns=['Name', 'Age', 'Country'])

appendDF = pd.DataFrame([
    ['Alpha', 10, 'India'],
    ['Beta', 15, 'Australia'],
    ['Gamma', 20, 'Canada'],
    ['Beta', 12, 'Australia'],
    ['Alpha', 10, 'India']
], columns=['Name', 'Age', 'Country'])

appendDFDropDupl = pd.DataFrame([
    ['Alpha', 10, 'India'],
    ['Beta', 15, 'Australia'],
    ['Gamma', 20, 'Canada'],
    ['Beta', 12, 'Australia'],
], columns=['Name', 'Age', 'Country'])

partitionByNameDF = pd.DataFrame([
    ['Gamma', 20, 'Canada'],
    ['Beta', 12, 'Australia'],
    ['Alpha', 10, 'India'],
], columns=['Name', 'Age', 'Country'])

partitionByNameAgeDF = pd.DataFrame([
    ['Beta', 15, 'Australia'],
    ['Gamma', 20, 'Canada'],
    ['Beta', 12, 'Australia'],
    ['Alpha', 10, 'India'],
], columns=['Name', 'Age', 'Country'])


class TestClass:
    def test_readExistingDataCSV(self):
        ac = AC.Cacher('csv', location=testDataLoc)
        df = ac.readExistingData()
        pd.testing.assert_frame_equal(df, sampleDF)

    def test_writeModeOverwrite(self):
        ac = AC.Cacher('csv', location=testDataLoc, writeMode='overwrite')
        df = ac.combineWithExistingData(newDF)
        pd.testing.assert_frame_equal(df, newDF)

    def test_writeModeAppend(self):
        ac = AC.Cacher('csv',
            location=testDataLoc,
            writeMode='append',
            dropDuplicates=False
        )
        df = ac.combineWithExistingData(newDF)
        df = df.reset_index(drop=True)
        pd.testing.assert_frame_equal(df, appendDF)
        ac = AC.Cacher('csv',
            location=testDataLoc,
            writeMode='append',
            dropDuplicates=True
        )
        df = ac.combineWithExistingData(newDF)
        df = df.reset_index(drop=True)
        pd.testing.assert_frame_equal(df, appendDFDropDupl)

    def test_writeModePartitionByCols(self):
        ac = AC.Cacher('csv',
            location=testDataLoc,
            writeMode='partitionByColumns',
            partitionCols=['Name', 'Age'],
            dropDuplicates=True
        )
        df = ac.combineWithExistingData(newDF)
        df = df.reset_index(drop=True)
        pd.testing.assert_frame_equal(df, partitionByNameAgeDF)
        ac = AC.Cacher('csv',
            location=testDataLoc,
            writeMode='partitionByColumns',
            partitionCols=['Name'],
            dropDuplicates=True
        )
        df = ac.combineWithExistingData(newDF)
        df = df.reset_index(drop=True)
        pd.testing.assert_frame_equal(df, partitionByNameDF)
