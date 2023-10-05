import streamlit as st
# Using HTML for a header with precise font size control
layout="wide"
initial_sidebar_state="expanded"
primaryColor="#C5BEBE"
backgroundColor="#FFFFFF"
secondaryBackgroundColor="#B36268"
textColor="#0E0E0E"
font="serif"

st.write('<h1 style="font-size: 46px;">Data Harvest Nexus</h1>', unsafe_allow_html=True)
import streamlit as st

# Introduction
st.subheader("YouTube Data Analyzer")
st.write("YouTube Data Analyzer is a cutting-edge and user-centric web application thoughtfully designed for a diverse audience ranging from social media users to seasoned advertising professionals and data analysts. At its core, this innovative platform provides you with seamless access, powerful analytics, and efficient data management capabilities for multiple YouTube channels. Whether you want to closely monitor channel growth, evaluate video performance, or delve into granular data analytics, YouTube Data Analyzer is your all-in-one solution.")




# Feature-Packed Capabilities
st.subheader("Feature-Packed Capabilities")

# Feature 1: Streamlined Data Access
st.subheader("1. Streamlined Data Access")
st.write("YouTube Data Analyzer makes it easier to access YouTube data. Users can easily retrieve comprehensive information about any YouTube channel with a few clicks. With ease, delve into channel analytics, video statistics, and audience insights.")
# Feature 2: Comprehensive Analytics
st.subheader("2. Comprehensive Analytics")
st.write("Utilise YouTube analytics to its greatest extent to gain a competitive advantage. Track channel subscriber numbers, video views, likes, dislikes, and comments to identify trends, improve content, and improve interaction tactics.")

# Feature 3: Data Management
st.subheader("3. Data Management")
st.write("You can effectively manage and store the gathered data in a safe and organised manner with the help of YouTube Data Analyzer. Data availability when you need it is ensured by the built-in data lake powered by MongoDB.")
# Feature 5: Seamless Migration
st.subheader("4. Seamless Migration")
st.write("Elevate your analysis by migrating data from the data lake to a SQL database as structured tables. This feature enables advanced querying, joining tables, and extracting valuable insights with precision.")

