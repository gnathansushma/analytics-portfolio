#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import pandas as pd

# connect to database
conn = sqlite3.connect("olist.sqlite")

# check tables
query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql(query, conn)

tables


# In[3]:


orders = pd.read_sql("SELECT * FROM orders", conn)
orders.head()


# In[4]:


#Funnel by Order status

query = """
SELECT order_status, COUNT(*) as total_orders
FROM orders
GROUP BY order_status
ORDER BY total_orders DESC
"""
df = pd.read_sql(query, conn)

df


# In[5]:


query = """
SELECT 
    o.order_id,
    o.order_status,
    p.payment_type,
    p.payment_value
FROM orders o
JOIN order_payments p
ON o.order_id = p.order_id
LIMIT 10
"""
df = pd.read_sql(query, conn)

df.head()


# In[12]:


#Checkout success rate
query = """
SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE 
        WHEN order_status = 'delivered' THEN order_id 
    END) AS delivered_orders
FROM orders
"""
df = pd.read_sql(query, conn)

df['conversion_rate'] = df['delivered_orders'] / df['total_orders']
df


# In[14]:


#Time based drop-off
query = """
SELECT 
    order_status,
    AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)) AS avg_delivery_days
FROM orders
WHERE order_status = 'delivered'
GROUP BY order_status
"""
df = pd.read_sql(query, conn)
df


# In[18]:


#Payment Funnel
query = """
SELECT 
    p.payment_type,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.order_status = 'delivered' THEN o.order_id END) AS successful_orders
FROM orders o
JOIN order_payments p
ON o.order_id = p.order_id
GROUP BY p.payment_type
ORDER BY total_orders DESC
"""
df = pd.read_sql(query, conn)

df['conversion_rate'] = df['successful_orders'] / df['total_orders']
df


# In[27]:


#Orders per customer
query = """
SELECT 
    c.customer_unique_id,
    COUNT(o.order_id) AS order_count
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_unique_id
"""
cust = pd.read_sql(query, conn)
cust


# In[28]:


# Customer segmentation - behavioral
def segment(x):
    if x == 1:
        return "One-time"
    elif x <= 5:
        return "Repeat"
    else:
        return "Loyal"

cust['segment'] = cust['order_count'].apply(segment)

cust['segment'].value_counts()


# In[29]:


#Geo - segmentation
query = """
SELECT 
    c.customer_state,
    COUNT(o.order_id) AS total_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_orders DESC
LIMIT 10
"""
df = pd.read_sql(query, conn)
df


# In[30]:


#High value customers
query = """
SELECT 
    o.customer_id,
    SUM(p.payment_value) AS total_spent
FROM orders o
JOIN order_payments p
ON o.order_id = p.order_id
GROUP BY o.customer_id
ORDER BY total_spent DESC
LIMIT 10
"""
df = pd.read_sql(query, conn)
df


# In[33]:


#For Dashboarding
query = """
SELECT 
    o.order_id,
    o.order_status,
    p.payment_type,
    p.payment_value,
    c.customer_unique_id,
    c.customer_state
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
JOIN order_payments p
ON o.order_id = p.order_id
"""
df = pd.read_sql(query, conn)


# In[34]:


cust_orders = df.groupby('customer_unique_id')['order_id'].nunique().reset_index()
cust_orders.columns = ['customer_unique_id', 'order_count']

df = df.merge(cust_orders, on='customer_unique_id')

df['segment'] = df['order_count'].apply(lambda x: 'One-time' if x==1 else 'Repeat')


# In[35]:


df.to_csv("final_dashboard_data.csv", index=False)


# In[ ]:




