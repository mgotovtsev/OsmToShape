import os
import sys
import struct
import math
from parameters import *

class HddHashTbl():

    def __init__(self, sHashTblName = 'hTable.hsh', sRowFormat = 'qq', nKeyCount = -1):

        self.sScriptPath     = os.path.dirname(sys.argv[0])
        self.sHshTblFileName = sHashTblName
        self.sHshTblFilePath = os.path.join(sTempDir, self.sHshTblFileName)

        self.sRowFormat    = sRowFormat
        self.nRowSize      = struct.calcsize(self.sRowFormat)

        self.sHeaderFormat = 'qq'
        self.nHeaderSize = struct.calcsize(self.sHeaderFormat)

        self.nOffsetForRead = 0
        self.HashTblFile    = None

        if os.path.exists(self.sHshTblFilePath) and nKeyCount == -1:
            self.OpenHshTblFile()
            self.nOffsetForRead, nKeyCount = self.GetHeader()

        self.nTotalCountRows         = nKeyCount
        self.nExtendedCountRows      = int(self.nTotalCountRows * 4)
        self.nHashTblActualSize      = 2 ** (int(math.ceil(math.log(self.nExtendedCountRows, 2))))
        self.nHastTblFileSize        = self.nRowSize * self.nHashTblActualSize + self.nHeaderSize

    def SetHeader(self, tplValues):
        self.HashTblFile.seek(0)
        RowWrite = struct.pack(self.sHeaderFormat, *tplValues)
        self.HashTblFile.write(RowWrite)

    def GetHeader(self):
        self.HashTblFile.seek(0)
        ReadedValue = self.HashTblFile.read(self.nHeaderSize)
        return struct.unpack(self.sHeaderFormat, ReadedValue)

    def SetOffsetForRead(self, nOffsetForRead = 0):
        self.HashTblFile.seek(0)
        RowWrite = struct.pack('q', nOffsetForRead)
        self.HashTblFile.write(RowWrite)

    def GetOffsetForRead(self):
        self.HashTblFile.seek(0)
        ReadedValue = self.HashTblFile.read(8)
        self.nOffsetForRead = struct.unpack('q', ReadedValue)

    def ReadRowFromHashFile(self, nOffsetIndex):
        self.HashTblFile.seek(nOffsetIndex * self.nRowSize + self.nHeaderSize)
        ReadedValue = self.HashTblFile.read(self.nRowSize)
        return struct.unpack(self.sRowFormat, ReadedValue)

    def CollisionDecision (self, Perturb, nKey, PerturbShift, nOffsetIndex):
        if Perturb == 0:
            Perturb = nKey
        else:
            Perturb >>= PerturbShift

        nSupposedIndex = (nOffsetIndex * 5) + 1 + Perturb

        nSupposedIndex = (nSupposedIndex & (self.nHashTblActualSize - 1))

        return nSupposedIndex, Perturb

    def WriteRowIntoHshTable(self, nOffsetIndex, nKey, tplValue, Perturb = 0, PerturbShift = 0):
        TargetValue = self.ReadRowFromHashFile(nOffsetIndex)
        if TargetValue[0] in [0, nKey]:
            self.HashTblFile.seek(nOffsetIndex * self.nRowSize + self.nHeaderSize)
            RowWrite = struct.pack(self.sRowFormat, nKey, *tplValue)
            self.HashTblFile.write(RowWrite)
        else:
            nSupposedIndex, Perturb = self.CollisionDecision(Perturb, nKey, PerturbShift, nOffsetIndex)
            self.WriteRowIntoHshTable(nSupposedIndex, nKey, tplValue, Perturb, 5)

    def ReadRowFromHshTbl(self, nOffsetIndex, nKey, Perturb = 0, PerturbShift = 0):
        TargetValue = self.ReadRowFromHashFile(nOffsetIndex)
        if TargetValue[0] == nKey:
            return TargetValue[1:]
        elif TargetValue[0] == 0:
            return None
        else:
            nSupposedIndex, Perturb = self.CollisionDecision(Perturb, nKey, PerturbShift, nOffsetIndex)
            return self.ReadRowFromHshTbl(nSupposedIndex, nKey, Perturb, 5)

    def HashValue(self, nKey):
        return nKey & (self.nHashTblActualSize-1)

    def GetValue(self, nKey):
        nSupposedIndex = self.HashValue(nKey)
        return self.ReadRowFromHshTbl(nSupposedIndex, nKey)

    def SetValue(self, nKey, tplValue = ()):
        nSupposedIndex = self.HashValue(nKey)
        self.WriteRowIntoHshTable(nSupposedIndex, nKey, tplValue)

    def CreateHshTblFile(self, sHshTblFilePath = ''):
        if sHshTblFilePath == '':
            sHshTblFilePath = self.sHshTblFilePath

        self.HashTblFile = open(sHshTblFilePath, "wb+")
        self.HashTblFile.truncate(self.nHastTblFileSize)
        self.SetHeader((self.nOffsetForRead, self.nTotalCountRows))

    def OpenHshTblFile(self, sHshTblFilePath = ''):
        if sHshTblFilePath == '':
            sHshTblFilePath = self.sHshTblFilePath
        self.HashTblFile = open(sHshTblFilePath, "rb+")

    def FreeHshTbl(self):
        self.HashTblFile.close()

    def __setitem__(self, key, item):
        self.SetValue(key, item)

    def __getitem__(self, Key):
        return self.GetValue(Key)

    def __del__(self):
        self.FreeHshTbl()

class BinHddHashTbl():
    def __init__(self, sHashTblName = 'bhTable.bhsh', sRowFormat = 'qq', CreateNewHashTable = False, RunCreateIndex = False):

        self.sHshTblFilePath = os.path.join(sTempDir, sHashTblName)
        self.sRowFormat      = sRowFormat
        self.nRowSize        = struct.calcsize(self.sRowFormat)

        self.sHeaderFormat   = 'qq'
        self.nHeaderSize     = struct.calcsize(self.sHeaderFormat)

        self.HashTblFile     = None
        self.TotalValuesCnt  = 0
        self.nOffsetForRead  = 0

        self.SortFunction       = lambda x: x[0]
        self.CreateNewHashTable = CreateNewHashTable
        self.RunCreateIndex     = RunCreateIndex
        self.BinHashIndex       = []

        self.OpenHshTblFile()

        if self.RunCreateIndex == True and CreateNewHashTable == False:
           self.CreateIndex()

    def SetHeader(self, tplValues):
        self.HashTblFile.seek(0)
        RowWrite = struct.pack(self.sHeaderFormat, *tplValues)
        self.HashTblFile.write(RowWrite)

    def GetHeader(self):
        nOldOffset = self.HashTblFile.tell()
        self.HashTblFile.seek(0)
        ReadedValue = self.HashTblFile.read(self.nHeaderSize)
        self.HashTblFile.seek(nOldOffset)
        return struct.unpack(self.sHeaderFormat, ReadedValue)

    def SetOffsetForRead(self, nOffsetForRead = 0):
        self.HashTblFile.seek(0)
        RowWrite = struct.pack('q', nOffsetForRead)
        self.HashTblFile.write(RowWrite)

    def GetOffsetForRead(self):
        nOldOffset = self.HashTblFile.tell()
        self.HashTblFile.seek(0)
        ReadedValue = self.HashTblFile.read(8)
        self.nOffsetForRead = struct.unpack('q', ReadedValue)
        self.HashTblFile.seek(nOldOffset)
        return self.nOffsetForRead

    def SetValue(self, nKey, tplValue):
        RowWrite = struct.pack(self.sRowFormat, nKey, *tplValue)
        self.HashTblFile.write(RowWrite)
        self.TotalValuesCnt += 1

    def SetListValues(self, listItems = [[]]):
        if len(listItems) > 0:
            listItems.sort(key = self.SortFunction)
            for listItemForInsert in listItems:
                self.SetValue(listItemForInsert[0], listItemForInsert[1:])

    def GetKeyByIndex(self, nIndex):
        self.HashTblFile.seek(nIndex * self.nRowSize + self.nHeaderSize)
        ReadedValue = self.HashTblFile.read(self.nRowSize)
        return struct.unpack(self.sRowFormat, ReadedValue)[0]

    def GetValueByIndex(self, nIndex):
        self.HashTblFile.seek(nIndex * self.nRowSize + self.nHeaderSize)
        ReadedValue = self.HashTblFile.read(self.nRowSize)

        return struct.unpack(self.sRowFormat, ReadedValue)[1:]

    def CreateIndex(self):
        self.BinHashIndex = []
        nChunksCount = round(pow(self.TotalValuesCnt, 0.3), 0)

        self.HashTblFile.seek(0)
        self.HashTblFile.read(self.nHeaderSize)
        #nReadOffset = self.nHeaderSize

        nRowCounter = 0
        ReadedValue = self.HashTblFile.read(self.nRowSize)

        # Read Bin Hash table into list
        while ReadedValue:

            if (nRowCounter % nChunksCount == 0
                or nRowCounter == self.TotalValuesCnt - 1): # for add to index last key

                tplReadedValue = struct.unpack(self.sRowFormat, ReadedValue)
                self.BinHashIndex.append(tplReadedValue + (nRowCounter,))

            ReadedValue = self.HashTblFile.read(self.nRowSize)
            ##nReadOffset += self.nRowSize
            nRowCounter += 1

        self.__getitem__ = self.__getitem_ind__

    def BinSearchValueInd(self, nSearchKey, nLowElement = 0, nHightElement = 0):

        if nHightElement == 0:
           nHightElement = len(self.BinHashIndex)

        while nLowElement < nHightElement:
            nMidIndex = (nLowElement + nHightElement) // 2
            nMidValue = self.BinHashIndex[nMidIndex][0]

            if nMidValue < nSearchKey:
                nLowElement = nMidIndex + 1
                nLastStep = 0
            elif nMidValue > nSearchKey:
                nHightElement = nMidIndex
                nLastStep = -1
            else:
                return self.BinHashIndex[nMidIndex][1:-1] # return result without key and row index

        return self.BinSearchValue(nSearchKey, self.BinHashIndex[nMidIndex + nLastStep][-1], self.BinHashIndex[nMidIndex + nLastStep + 1][-1])

    def BinSearchValue(self, nSearchKey, nLowElement = 0, nHightElement = 0):

        if nHightElement == 0:
           nHightElement = self.TotalValuesCnt

        while nLowElement < nHightElement:
            nMidIndex = (nLowElement + nHightElement) // 2
            nMidValue = self.GetKeyByIndex(nMidIndex)

            if nMidValue < nSearchKey:
                nLowElement = nMidIndex + 1
            elif nMidValue > nSearchKey:
                nHightElement = nMidIndex
            else:
                return self.GetValueByIndex(nMidIndex)

        return None

    def OpenHshTblFile(self):
        if os.path.exists(self.sHshTblFilePath) and self.CreateNewHashTable == False:
            self.HashTblFile = open(self.sHshTblFilePath, "rb+")
            self.nOffsetForRead, self.TotalValuesCnt = self.GetHeader()
        else:
            self.HashTblFile = open(self.sHshTblFilePath, "wb+")
            self.SetHeader((self.nOffsetForRead, self.TotalValuesCnt))

    def FreeHshTbl(self):
        self.HashTblFile.flush()
        self.SetHeader((self.nOffsetForRead, self.TotalValuesCnt))
        self.HashTblFile.close()

    def __setitem__(self, key, item):
        self.SetValue(key, item)

    def __getitem__(self, key):
        return self.BinSearchValue(key)

    def __getitem_ind__(self, key):
        return self.BinSearchValueInd(key)

    def __del__(self):
        self.FreeHshTbl()
