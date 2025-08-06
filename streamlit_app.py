import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

# Database setup
DATABASE_URL = st.secrets.get("DATABASE_URL", "sqlite:///rates.db")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class Rate(Base):
    __tablename__ = 'rates'
    id = Column(Integer, primary_key=True)
    bank = Column(String, index=True)
    currency = Column(String)
    buy = Column(Float)
    sell = Column(Float)
    date = Column(Date, index=True)

Base.metadata.create_all(engine)

# Scraper functions

def fetch_alif():
    url = 'https://alif.tj/'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    # Example selectors, adjust accordingly
    table = soup.find('table', {'class': 'exchange-rates'})
    records = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        currency = cols[0].text.strip()
        buy = float(cols[1].text.strip())
        sell = float(cols[2].text.strip())
        records.append((currency, buy, sell))
    return records


def fetch_humo():
    url = 'https://humo.tj/ru/'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    records = []
    for item in soup.select('.currency-rate-item'):
        currency = item.select_one('.currency-code').text.strip()
        buy = float(item.select_one('.rate-buy').text.strip())
        sell = float(item.select_one('.rate-sell').text.strip())
        records.append((currency, buy, sell))
    return records


def fetch_activbank():
    url = 'https://activbank.tj/tj'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    records = []
    rows = soup.find('table', {'id': 'fx-table'}).find_all('tr')[1:]
    for r in rows:
        cols = r.find_all('td')
        currency = cols[0].text.strip()
        buy = float(cols[1].text.strip())
        sell = float(cols[2].text.strip())
        records.append((currency, buy, sell))
    return records

# Store daily rates
def fetch_and_store():
    today = datetime.date.today()
    for bank, func in [('Alif', fetch_alif), ('Humo', fetch_humo), ('ActivBank', fetch_activbank)]:
        try:
            for currency, buy, sell in func():
                rate = Rate(bank=bank, currency=currency, buy=buy, sell=sell, date=today)
                session.add(rate)
            session.commit()
        except Exception as e:
            session.rollback()
            st.error(f"Error fetching {bank}: {e}")

# Streamlit UI
st.title("Tajik Bank Exchange Rates")

# Fetch and store on button click or schedule externally via cron
if st.button("Fetch and store today's rates"):
    fetch_and_store()
    st.success("Fetched and stored today's rates.")

# Sidebar filter
date = st.sidebar.date_input("Select Date", datetime.date.today())
banks = st.sidebar.multiselect("Banks", ['Alif', 'Humo', 'ActivBank'], default=['Alif', 'Humo', 'ActivBank'])
currency = st.sidebar.text_input("Currency", 'USD')

# Query data
df = pd.read_sql(
    session.query(Rate).filter(Rate.date == date, Rate.bank.in_(banks), Rate.currency == currency).statement,
    session.bind
)

st.subheader(f"Rates for {currency} on {date}")
st.write(df)

# Download CSV
csv = df.to_csv(index=False).encode('utf-8')
st.download_button("Download CSV", csv, file_name=f"rates_{currency}_{date}.csv")

# Historical graph
hist = pd.read_sql(
    session.query(Rate).filter(Rate.bank.in_(banks), Rate.currency == currency).statement,
    session.bind,
    parse_dates=['date']
)
hist = hist.pivot(index='date', columns='bank', values='buy')
st.subheader("Historical Buy Rates")
st.line_chart(hist)

# Instructions for scheduling
st.markdown("""

**Note:** To automate daily fetching, schedule `python streamlit_app.py fetch` via cron or use a task scheduler.

""")
