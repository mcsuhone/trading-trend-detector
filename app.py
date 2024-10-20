import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def generate_data(num_points, volatility):
    np.random.seed(42)  # For reproducibility
    dates = pd.date_range(start='2023-01-01', periods=num_points)
    price = 100 + np.cumsum(np.random.randn(num_points) * volatility)
    return pd.DataFrame({'Date': dates, 'Price': price})

def main():
    st.title('Trading Data Visualization')

    # Text input for stock symbol
    symbol = st.text_input('Enter Stock Symbol', 'EXMP')

    # Slider for volatility
    volatility = st.slider('Market Volatility', min_value=0.1, max_value=2.0, value=1.0, step=0.1)

    # Generate placeholder data
    data = generate_data(100, volatility)

    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(data['Date'], data['Price'])
    ax.set_title(f'{symbol} Stock Price')
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')

    # Display the plot in Streamlit
    st.pyplot(fig)

    # Display the data
    st.subheader('Raw Data')
    st.dataframe(data)

if __name__ == '__main__':
    main()