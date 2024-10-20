import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Improved data generation function
def generate_realistic_trading_data(num_symbols=100, start_date=datetime(2021, 11, 8), num_days=7):
    data = []
    exchanges = ['FR', 'NL', 'ETR']
    
    for symbol in range(num_symbols):
        exchange = np.random.choice(exchanges)
        sec_type = np.random.choice(['E', 'I'], p=[0.8, 0.2])
        
        # Initial price
        price = np.random.uniform(10, 1000)
        
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            
            # Generate prices for every 5 minutes
            for minute in range(0, 24*60, 5):
                timestamp = current_date + timedelta(minutes=minute)
                
                # Add some random walk to the price
                price += np.random.normal(0, price * 0.001)  # 0.1% standard deviation
                
                data.append({
                    'ID.[Exchange]': f'{symbol:04d}.{exchange}',
                    'SecType': sec_type,
                    'Last': price,
                    'Trading time': timestamp.strftime('%H:%M:%S'),
                    'Trading date': timestamp.date()
                })
    
    return pd.DataFrame(data)

# Cache the data loading
@st.cache_data
def load_or_generate_data():
    try:
        return pd.read_csv('trading_data.csv', parse_dates=['Trading date'])
    except FileNotFoundError:
        df = generate_realistic_trading_data()
        df.to_csv('trading_data.csv', index=False)
        return df

# Main function to run the Streamlit app
def main():
    st.title('Improved Trading Data Visualization')

    # Load the data
    data = load_or_generate_data()

    # Display basic information about the dataset
    st.subheader('Dataset Overview')
    st.write(f"Total number of records: {len(data)}")
    st.write(f"Number of unique symbols: {data['ID.[Exchange]'].nunique()}")
    st.write(f"Date range: {data['Trading date'].min()} to {data['Trading date'].max()}")

    # Allow user to select a symbol
    symbols = sorted(data['ID.[Exchange]'].unique())
    selected_symbol = st.selectbox('Select a symbol to visualize', symbols)

    # Filter data for the selected symbol
    symbol_data = data[data['ID.[Exchange]'] == selected_symbol].sort_values('Trading date')

    # Allow user to select date range
    date_range = st.date_input(
        "Select date range",
        value=(symbol_data['Trading date'].min(), symbol_data['Trading date'].max()),
        min_value=symbol_data['Trading date'].min(),
        max_value=symbol_data['Trading date'].max()
    )

    # Filter data based on selected date range
    filtered_data = symbol_data[
        (symbol_data['Trading date'] >= date_range[0]) &
        (symbol_data['Trading date'] <= date_range[1])
    ]

    # Display basic stats for the selected symbol
    st.subheader(f'Statistics for {selected_symbol}')
    st.write(f"Security Type: {filtered_data['SecType'].iloc[0]}")
    st.write(f"Number of data points: {len(filtered_data)}")
    st.write(f"Average price: {filtered_data['Last'].mean():.2f}")
    st.write(f"Price range: {filtered_data['Last'].min():.2f} to {filtered_data['Last'].max():.2f}")

    # Create a candlestick chart
    fig = go.Figure(data=[go.Candlestick(
        x=filtered_data['Trading date'] + pd.to_timedelta(filtered_data['Trading time']),
        open=filtered_data['Last'],
        high=filtered_data['Last'],
        low=filtered_data['Last'],
        close=filtered_data['Last']
    )])

    fig.update_layout(
        title=f'Trading Data for {selected_symbol}',
        xaxis_title='Date and Time',
        yaxis_title='Price',
        xaxis_rangeslider_visible=False
    )

    st.plotly_chart(fig)

    # Display the raw data
    if st.checkbox('Show raw data'):
        st.subheader('Raw data')
        st.write(filtered_data)

if __name__ == '__main__':
    main()