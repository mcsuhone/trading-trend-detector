import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def generate_data(num_points, seed):
    np.random.seed(seed)  # For reproducibility
    dates = pd.date_range(start='2023-01-01', periods=num_points)
    price = 100 + np.cumsum(np.random.randn(num_points))
    return pd.DataFrame({'Date': dates, 'Price': price})

def main():
    st.title('Trading Data Visualization')

    # Text input for stock symbol
    symbol = st.text_input('Enter Stock Symbol', 'EXMP')

    # Slider for volatility
    seed = st.slider('Market seed', min_value=0.1, max_value=100.0, value=1.0, step=0.1)
    seed = int(seed)

    # Generate placeholder data
    data = generate_data(100, seed)

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