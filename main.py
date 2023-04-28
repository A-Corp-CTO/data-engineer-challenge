# Import packages
import pandas as pd
import hashlib
import numpy as np
# Import data

Deals = pd.read_csv('data/deal_sample.csv')
Updates = pd.read_csv('data/deal_updates_sample.csv')
Activities = pd.read_csv('data/deal_activities_sample.csv')

# Transform
Deals=Deals.rename(columns={"id":"DealID","user_id":"UserID","Total_activities":"TotalActivities"})
Activities=Activities.rename(columns={"activity_id":"ActivityID","deal_id":"DealID","marked_as_done_ts":"MarkedAsDone","deleted":"Deleted"})
Updates=Updates.rename(columns={"deal_id":"DealID","update_type":"Type","old_value":"Old","new_value":"New"})
Deals = Deals.drop('pipeline_id',1)
#Deals['UserID']=Deals['UserID'].apply(lambda x: int(hashlib.sha256(x.encode('utf-8')).hexdigest(),16))

# Function to create the insert into table statements and add them to the create_statements.sql file
def auto_insert(dataframe, table):
    col_names = list(dataframe.columns)
    with open('create_statements.sql','a+') as outfile:
        for row in dataframe.itertuples(index=False):
            val = []
            for value in row:
                if pd.isnull(value):
                    val.append("NULL")
                elif isinstance(value,str):
                    val.append(f"'{value}'")
                else:
                    val.append(str(value))
            insert=(f"INSERT INTO {table}({','.join(col_names)}) VALUES({','.join(val)});\n")
            outfile.write(insert)


# auto_insert(Deals,"Deals")
# auto_insert(Updates,"Updates")
# auto_insert(Activities,"Activities")


##Here we want to check if the Total_activities column is accurate

# merge the Deals and Activities dataframes on the "DealID" column and filter out the rows
deal_activities = pd.merge(Deals, Activities, on="DealID", how="inner")
deal_activities = deal_activities[(deal_activities["Deleted"] == False) & (deal_activities["MarkedAsDone"].notnull())]
# group the merged dataframe by "DealID" and count the number of rows in each group
activity_counts = deal_activities.groupby("DealID").size()
# create a new column in the Deals dataframe with the activity count for each deal
Deals["ActivityCount"] = Deals["DealID"].map(activity_counts)
Deals = Deals.replace(np.nan,0)
Deals = Deals.astype({"ActivityCount":'int64'})

#Now we calculate the accuracy between the newly created column and Total_activities
Accuracy = 0
#Counting rows where the two columns are the same
for _,i in Deals.iterrows():
    if i['TotalActivities'] == i['ActivityCount']:
        Accuracy = Accuracy + 1
#Divide count of rows with same values by count of rows to get percentage accuracy
print("The accuracy for the Total_activities column is :",Accuracy/5000)


##Calculating average number of updates per deal only for status value and stage
#Code is the same structure as the one above
# merge the Deals and Activities dataframes on the "DealID" column
deals_updates = pd.merge(Deals, Updates, on="DealID", how="inner")
deals_updates = deals_updates[(deals_updates["Type"] == "value") | (deals_updates["Type"] == "stage_id")| (deals_updates["Type"] == "status")]
# group the merged dataframe by "DealID" and count the number of rows in each group
updates_count = deals_updates.groupby("DealID").size()

# create a new column in the Deals dataframe with the activity count for each "DealID"
Deals["UpdateCount"] = Deals["DealID"].map(updates_count)#
Deals = Deals.replace(np.nan,0)
Deals = Deals.astype({"UpdateCount":'int64'})

#Find average
print("The average amount of updates per deal is :",np.average(Deals['UpdateCount']))


##Adding a column called "Active" to mark Inactive deals
Deals['Active']= np.where((Deals['ActivityCount']==0) & (Deals['UpdateCount']==0),'Inactive','Active')


##Creating a dataframe that has the deals table plus 2 more columns for numCalls and numEmails (won deals only)
WonDeals = Deals[Deals['Status']=="won"]
#Same structure as before
wondealsactivitities = pd.merge(WonDeals, Activities, on="DealID", how="inner")
wondealsactivitities = wondealsactivitities[(wondealsactivitities["Type"]=='email')]
# group the merged dataframe by "DealID" and count the number of rows in each group
activity_email = wondealsactivitities.groupby("DealID").size()
WonDeals["EmailCount"] = WonDeals["DealID"].map(activity_email)
# merge the Deals and Activities dataframes on the "DealID" column
wondealsactivitities_1 = pd.merge(WonDeals, Activities, on="DealID", how="inner")
wondealsactivitities_1 = wondealsactivitities_1[(wondealsactivitities_1["Type"]=='call')]
# group the merged dataframe by "DealID" and count the number of rows in each group
activity_calls = wondealsactivitities_1.groupby("DealID").size()
# create a new column in the Deals dataframe with the activity count for each "DealID"
WonDeals["CallCount"] = WonDeals["DealID"].map(activity_calls)
WonDeals = WonDeals.replace(np.nan,0)
WonDeals = WonDeals.astype({"EmailCount":'int64',"CallCount":"int64"})
