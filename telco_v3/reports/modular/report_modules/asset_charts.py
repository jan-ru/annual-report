# Telco B.V. Asset Charts Module

print("## Asset Composition Analysis")
print()

# Extract asset data
real_estate = get_safe_value(balance_df, 'Vastgoedbeleggingen', current_year)
financial_assets = get_safe_value(balance_df, 'Financiële vaste activa', current_year)
receivables = get_safe_value(balance_df, 'Vorderingen', current_year)
securities = get_safe_value(balance_df, 'Effecten', current_year)
cash = get_safe_value(balance_df, 'Liquide middelen', current_year)

asset_categories = ['Real Estate Investments', 'Financial Assets', 'Receivables', 'Securities', 'Cash']
asset_values = [real_estate, financial_assets, receivables, securities, cash]

# Filter out zero values for the pie chart
non_zero_assets = [(cat, val) for cat, val in zip(asset_categories, asset_values) if val > 0]

if non_zero_assets:
    categories, values = zip(*non_zero_assets)
    
    # Asset composition pie chart
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    fig = go.Figure(data=[go.Pie(
        labels=categories,
        values=values,
        hole=0.4,
        marker_colors=colors[:len(categories)],
        textinfo='label+percent',
        textfont_size=10
    )])
    
    fig.update_layout(
        title=f"Asset Composition - {current_year}",
        height=500,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5)
    )
    
    fig.show()
    
    # Asset analysis
    total_assets = sum(values)
    print(f"### Asset Distribution Analysis")
    print()
    
    for category, value in zip(categories, values):
        percentage = (value / total_assets) * 100
        print(f"- **{category}:** {format_currency_eur(value)} ({percentage:.1f}%)")
    
    print()
    if real_estate > 0:
        real_estate_pct = (real_estate / total_assets) * 100
        print(f"Real estate investments represent {real_estate_pct:.1f}% of total assets, ")
        print("indicating the company's strategic focus on property investments.")
    
    if financial_assets > 0:
        fin_assets_pct = (financial_assets / total_assets) * 100
        print(f"Financial assets constitute {fin_assets_pct:.1f}% of the asset base, ")
        print("reflecting the company's investment and lending activities.")

else:
    print("No significant asset composition to analyze.")
