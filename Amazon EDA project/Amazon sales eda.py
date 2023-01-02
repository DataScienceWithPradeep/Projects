#!/usr/bin/env python
# coding: utf-8

# In[18]:


import sqlalchemy
import os
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')


# Connecting to SQL server to load the data

# In[19]:


# import the module
from sqlalchemy import create_engine

# create sqlalchemy engine0
engine = create_engine("mysql+pymysql://root:root password@localhost/amazon_data"
                       .format(user="root",
                               pw="root password",
                               db="amazon_data"))


# In[20]:


os.chdir(r"C:\Users\Pradeep Nishad\Downloads\Sales_Data")


# In[21]:


a = os.listdir()


# In[22]:


table_names=[]
for i in a:
    table_names.append(i.split(".")[0])


# In[23]:


# for i,j in zip(table_names,a):
#     data=pd.read_csv(j)
#     data.to_sql(i,engine,index=False)


# In[24]:


engine.table_names()


# In[25]:


df = pd.DataFrame()


# In[26]:


df2=pd.read_csv(r'C:\Users\Pradeep Nishad\Downloads\Sales_Data\Sales_December_2019.csv')
df=pd.concat([df,df2],axis=0,ignore_index=True)


# In[27]:


#appending the sql data into pandas dataframe
for i in engine.table_names():
    data = pd.read_sql_table(i,engine)
    df=pd.concat([df,data],axis=0,ignore_index=True)


# In[28]:


df


# In[29]:


df1=df.copy()


# STEP 1: Preproccesing the data

# In[30]:


df.info()


# In[31]:


#checking for errors when converting the data types
for i in df["Price Each"]:
    try:
        if type(float(i))==float:
            continue
    except Exception as e:
        print(e,i)


# In[32]:


for i in df["Quantity Ordered"]:
    try:
        if type(int(i))==int:
            continue
    except Exception as e:
        print(e)


# In[33]:


# droping the unnecessary string and null values from the data
df = df.loc[df['Quantity Ordered'] != 'Quantity Ordered']
df = df.loc[df['Quantity Ordered'].isnull()==False]
df = df.reset_index(drop = True)


# In[34]:


# converting the necessary coloumns to relevant data type
df['Quantity Ordered'] = df['Quantity Ordered'].astype('int32')
df['Price Each'] = df['Price Each'].astype('float32')
df['Order Date'] = pd.to_datetime(df['Order Date'])


# In[35]:


df.info()


# STEP 2: Adding the necessary coloumns

# In[36]:


df["Sales"] = df['Quantity Ordered']*df['Price Each']
df["Month"] = df["Order Date"].dt.month
df["Day"] = df["Order Date"].dt.day
df["Hour"] = df["Order Date"].dt.hour
df["Day Name"] = df["Order Date"].dt.day_name()


# In[37]:


df.sample(10)


# In[38]:


df2 = df.copy()


# Analysis 1: Checking the most relevant time, date and month for a annual campaign of 3 days

# In[39]:


# extracting the monthly sales and quantity ordered
monthly_sales = pd.DataFrame(df.groupby('Month')['Sales'].sum())
monthly_order = pd.DataFrame(df.groupby('Month')['Quantity Ordered'].sum())
monthly_sales = monthly_sales.reset_index(drop=False)
monthly_order = monthly_order.reset_index(drop=False)


# In[40]:


color_names=["red","orange","blue","green","yellow","pink","Maroon","purple","olive","brown","gray","cyan"]
month_names = ["jan","feb","march","april","may","june","july","aug","sep","oct","nov","dec"]


# In[41]:


monthly_sales["month_name"] = [i for i in month_names]
monthly_order["month_name"] = [i for i in month_names]


# In[42]:


monthly_sales


# In[43]:


fig,ax1 = plt.subplots(figsize = (18,5))
ax2 = ax1.twinx()
ax1.bar(monthly_sales["month_name"],monthly_sales["Sales"],color=color_names)
ax2.plot(monthly_order["month_name"],monthly_order["Quantity Ordered"]
         , color="black")
plt.xticks(monthly_sales["month_name"])
ax1.set_xlabel("month")
ax1.set_ylabel("sales in USD($)",color="darkred")
ax2.set_ylabel("Quantity Ordered", color="darkgreen")
plt.show()


# In[44]:


# extracting the daily sales and quantity ordered
daily_sales = pd.DataFrame(df.groupby('Day')['Sales'].sum())
daily_qty = pd.DataFrame(df.groupby('Day')['Quantity Ordered'].sum())
daily_sales = daily_sales.reset_index(drop=False)
daily_qty = daily_qty.reset_index(drop=False)
daily_sales


# In[45]:


fig1,ax1 = plt.subplots(figsize = (18,5))
ax2 = ax1.twinx()
ax1.bar(daily_sales["Day"],daily_sales["Sales"],color=color_names)
ax2.plot(daily_qty["Day"],daily_qty["Quantity Ordered"]
         , color="black")
plt.xticks(daily_sales["Day"])
ax1.set_xlabel("Day")
ax1.set_ylabel("sales in USD($)",color="darkred")
ax2.set_ylabel("Quantity Ordered", color="darkgreen")
plt.show()


# In[46]:


# extracting the hourly sales and quantity ordered
hourly_sales = pd.DataFrame(df.groupby('Hour')['Sales'].sum())
hourly_qty = pd.DataFrame(df.groupby('Hour')['Quantity Ordered'].sum())
hourly_sales = hourly_sales.reset_index(drop=False)
hourly_qty = hourly_qty.reset_index(drop=False)
hourly_sales


# In[47]:


fig2,ax1 = plt.subplots(figsize = (18,5))
ax2 = ax1.twinx()
ax1.bar(hourly_sales["Hour"],hourly_sales["Sales"],color=color_names)
ax2.plot(hourly_qty["Hour"],hourly_qty["Quantity Ordered"]
         , color="black")
plt.xticks(hourly_sales["Hour"])
ax1.set_xlabel("Hour")
ax1.set_ylabel("sales in USD($)",color="darkred")
ax2.set_ylabel("Quantity Ordered", color="darkgreen")
plt.show()


# 

# Analysis 2: Check the most sold products in different city to manage the warehousing.

# In[48]:


# Extracting city and state from the address in the new column
df['City_State'] = df['Purchase Address'].apply(lambda i:i.split(',')[1]+' '+i.split(',')[2].split(' ')[1])


# In[49]:


# alternate method 1 for city extraction
# l1 = []
# for i in df["Purchase Address"]:
#     l1.append(i.split(',')[1]+ " " + i.split(',')[2].split(' ')[1])
# df["City"] = l1


# In[50]:


# alternate method 2 for city extraction
# for i,j in enumerate(df["Purchase Address"]):
#     df["Purchase Address"][i] = (j.split(',')[1]+ " " + j.split(',')[2].split(' ')[1])


# In[51]:


# extracting quantity ordered in each city Prodcut wise as sales is the secondary parameter for our problem statement
city_order = pd.DataFrame(df.groupby(["City_State","Product"])['Quantity Ordered'].sum())
city_order = city_order.reset_index(drop=False)


# In[52]:


city_order


# In[53]:


best=city_order.sort_values(['City_State','Quantity Ordered'],ascending=False)


# In[54]:


top_5=best.groupby("City_State").tail(10)


# In[55]:


top_5=top_5.reset_index(drop=True)


# In[56]:


pd.set_option("display.max_rows",None)


# In[57]:


import seaborn as sns


# In[58]:


plt.figure(figsize = (18,5))
sns.barplot(x=top_5['City_State'],y=top_5['Quantity Ordered'],hue=top_5['Product'],palette="tab20")
plt.show()


# In[ ]:





# In[59]:


# most_order = pd.DataFrame(df.groupby(["City_State","Product"])['Quantity Ordered'].nlargest(5))


# In[60]:


# most_order


# In[61]:


city_order.nlargest(5,["Quantity Ordered"])


# Analysis 3: Check the most products bought together for better recomendation to customers

# In[62]:


len(pd.unique(df['Order ID']))


# In[63]:


#extracting all the order id which have ordered multiple products
duplicates=df[df['Order ID'].duplicated(keep=False)]


# In[64]:


duplicates


# In[65]:


duplicates["group"]=duplicates.groupby('Order ID')['Product'].transform(lambda x: ",".join(x))


# In[66]:


duplicates


# In[67]:


from itertools import combinations
from collections import Counter


# In[68]:


count = Counter()

for row in duplicates["group"]:
    row_list = row.split(",")
    count.update(Counter(combinations(row_list, 2)))


# In[69]:


com=pd.DataFrame.from_dict(count,orient="index",columns=['Count'])


# In[70]:


com=com.reset_index(drop=False)


# In[71]:


com


# In[72]:


com.rename(columns={'index':'Combinations'},inplace=True)


# In[73]:


com=com.sort_values(by="Count",ascending=False)


# In[74]:


plt.figure(figsize=(25,16),dpi=400)
plot=sns.barplot(com['Combinations'].head(10),com['Count'],palette="tab20")
plot.set_xticklabels(labels = com['Combinations'].head(10),rotation = 90)
plot.tick_params(axis = "both",which = "major", labelsize=20)
plt.show()


# In[ ]:





# In[ ]:


#Define - KPI , CTQ
#Measure
#Analysis
#Improve
#Control
#times roman - 12 - header(14)

