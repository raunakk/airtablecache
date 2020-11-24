import airtable as AT
import gspread as GS
import gspread_dataframe as GD
import pandas as pd
import json
import os


class Cacher:
    def __init__(
        self,
        cacheType: str,  # Supports csv, gsheet
        **kwargs
    ):
        self.kwargs = kwargs
        self.cacheType = cacheType

        if cacheType == "csv":
            if "location" not in kwargs.keys():
                raise ValueError("\"location\" keyword needs to be specified")
        elif cacheType == "gsheet":
            reqKeys = ['workbook', 'worksheet', 'authDict']
            missing = set(reqKeys) - set(kwargs.keys())
            if len(missing):
                raise ValueError("{} args need to be provided".format(",".join(list(missing))))
        else:
            raise NotImplementedError("{} CacheType is not support".format(cacheType))

        self.writeMode = self.kwargs.get("writeMode", "append")
        if self.writeMode == "partitionByColumns":
            if "partitionCols" not in kwargs.keys():
                raise ValueError("partitionCols needs to be provided for partitionByColumns writeMode")

        self.storeFuncMap = {
            "csv": self.storeCSV,
            "gsheet": self.storeGSheet
        }
        self.readExistingDataFuncMap = {
            "csv": self.readCSVData,
            "gsheet": self.readGSheetData
        }

    def initializeGoogleServiceAcc(self):
        with open(self.kwargs['secresFilePath'], 'r') as f:
            fileContents = f.read()
        self.secrets = json.load(fileContents)
        creds = GS.auth.ServiceAccountCredentials.from_service_account_info(
            self["secrets"],
            scopes=GS.auth.DEFAULT_SCOPES
        )
        self.googleSA = GS.Client(auth=creds)

    def readExistingData(self):
        func = self.readExistingDataFuncMap[self.cacheType]
        df = func()
        return df

    def readCSVData(self):
        readLoc = self.kwargs.get("readLoc", self.kwargs['location'])
        if not os.path.exists(readLoc):
            return None
        df = pd.read_csv(readLoc)
        return df

    def readGSheetData(self):
        self.initializeGoogleServiceAcc()
        gc = self.googleSA
        wb = gc.open(self.kwargs['workbook'])
        ws = wb.worksheet(self.kwargs['worksheet'])
        existing = GD.get_as_dataframe(ws)
        return existing

    def combineWithExistingData(self, df: pd.DataFrame):
        if self.writeMode == "overwrite":
            return df
        else:
            existing = self.readExistingData()
            if existing is None:
                return df
            else:
                if self.writeMode == "append":
                    if existing is not None:
                        df = pd.concat([existing, df])

                elif self.writeMode == "partitionByColumns":
                    if self.kwargs['partitionCols'] == 'all':
                        partitionCols = df.columns
                    else:
                        partitionCols = self.kwargs['partitionCols']
                    df = df.set_index(partitionCols)
                    existing = existing.set_index(partitionCols)
                    existing = existing.drop(df.index, errors='ignore')
                    existing = existing.reset_index()
                    df = df.reset_index()
                    df = pd.concat([existing, df])

                else:
                    raise NotImplementedError(self.writeMode + " writeMode not implemented")
        if self.kwargs.get("dropDuplicates", False):
            df = df.drop_duplicates(ignore_index=True)
        return df

    def storeData(self, df: pd.DataFrame):
        storeFunc = self.storeFuncMap.get(self.cacheType)
        storeFunc(df)

    def storeCSV(self, df: pd.DataFrame):
        storeLoc = self.kwargs.get('writeLoc', self.kwargs['location'])
        df.to_csv(storeLoc)

    def storeGSheet(self, df: pd.DataFrame):
        self.initializeGoogleServiceAcc()
        gc = self.googleSA
        wb = gc.open(self.kwargs['workbook'])
        ws = wb.worksheet(self.kwargs['worksheet'])
        GD.set_with_dataframe(ws, df, resize=self.kwargs.get("resize", True))


class AirtableCache:
    def __init__(
        self,
        atObj: AT.Airtable,
        cacheType: str,  # Supports csv, gsheet
        **kwargs
    ):
        self.atObj = atObj
        self.atGetAllArgs = kwargs.get("atKWArgs", {})
        self.kwargs = kwargs
        self.cacheObj = Cacher(cacheType, **kwargs)

    def getData(self):
        records = self.atObj.get_all(**self.kwargs)
        df = pd.DataFrame.from_dict([x['fields'] for x in records])
        return df

    def storeData(self):
        df = self.getData()
        if self.kwargs.get("processDataFunc", None) is not None:
            df = self.kwargs.get("processDataFunc", None)(df, self.kwargs.get("processDataContext", {}))
        df = self.cacheObj.combineWithExistingData(df)
        self.cacheObj.storeData(df)
