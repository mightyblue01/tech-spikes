import json

class EventDataReconciliation():
  def generateComparisonResults(self, apiDict, seedingDict, commonDataSet, comparisonResultsFile):
    count=0
    with open(comparisonResultsFile, 'w+') as outFile:
      for val in commonDataSet:
        apiVal= apiDict.get(val)
        seedingVal = seedingDict.get(val)
      
        if(apiVal['updated'] == seedingVal['updated']):
          line1=json.dumps(apiVal)
          line2=json.dumps(seedingVal)
          resultLine=line1+line2
          outFile.write(resultLine)
          count=count+1
          
      print("Total unchanged events(same id and updated fields) %d " % count)
      logger.info('PredictHQDataValidation: Total unchanged events(same id and updated fields) {}'.format(commonDataSetLen))

        
  def compareEventData(self, apiDumpFile, seedingDumpFile, comparisonResultsFile):
    apiDict={}
    with open(apiDumpFile) as f1:
      for line1 in f1:
        ins1=json.loads(line1)
        apiDict[ins1['id']]=ins1
    print("done creating input dictionary")

    seedingDict={}
    with open(seedingDumpFile) as f2:
      for line2 in f2:
        ins2=json.loads(line2)
        seedingDict[ins2['id']]=ins2
    print("done creating seeding dictionary")

    commonDataSet=apiDict.keys() & seedingDict.keys()
    commonDataSetLen=len(commonDataSet)
    
    print("Total events with common id in both datasets  " % commonDataSetLen)
    logger.info('PredictHQDataValidation: Total events with common id in both datasets {}'.format(commonDataSetLen))

    self.generateComparisonResults(apiDict, seedingDict, commonDataSet, comparisonResultsFile)
    
    #call reconciliation module 
    apiDumpFile="file-1-path"
    seedingDumpFile="file-2-path"
    comparisonResultsFile="result-file-path"

    b=EventDataReconciliation()
    testresult=b.compareEventData(apiDumpFile, seedingDumpFile, comparisonResultsFile)    
  
    
        
        
