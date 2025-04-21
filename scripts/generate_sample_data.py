# scripts/generate_sample_data.py
# ... (imports)

def generate_synthetic_sales_data(...): # Args same as before
    # ... (setup fake, states, dates)
    sales_channels = ['Direct', 'Web', 'Amazon-FBA', 'Retail', 'Etsy', 'Wholesale']
    channel_weights = [0.4, 0.3, 0.1, 0.1, 0.05, 0.05]

    data = []
    for i in range(num_records):
        # ... (generate date, customer, address, state, zip)

        # Generate amount, include occasional returns/credits (e.g., 5% chance)
        is_return = random.random() < 0.05
        amount = round(random.uniform(min_amount, max_amount), 2)
        if is_return:
            amount = -abs(amount) # Make it negative

        # Generate unique enough invoice number
        invoice_num = f"INV-{random.randint(10000, 99999)}-{i}"
        if is_return:
            invoice_num = f"CREDIT-{invoice_num}" # Optional: indicate credit memo

        # Select sales channel
        channel = np.random.choice(sales_channels, p=channel_weights)

        data.append({
            'date': trans_date.strftime('%Y-%m-%d %H:%M:%S'),
            'invoice_number': invoice_num,
            'invoice_date': invoice_date.strftime('%Y-%m-%d'),
            'total_amount': amount, # Now includes negative values
            'customer_name': customer_name,
            'street_address': address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'sales_channel': channel # Added channel
        })
        # ... (progress print)

    df = pd.DataFrame(data)
    # Ensure schema matches expected input
    df = df[[
        'date', 'invoice_number', 'invoice_date', 'total_amount', 'customer_name',
        'street_address', 'city', 'state', 'zip_code', 'sales_channel' # Updated columns
    ]]
    # ... (save to CSV, print summary)
    logging.info(f"Generated {len(df[df['total_amount'] < 0])} return/credit records.")

# ... (main execution block)