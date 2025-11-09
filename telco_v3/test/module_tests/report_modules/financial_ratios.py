# Telco B.V. Financial Ratios Module

print("## Financial Ratios and Key Performance Indicators")
print()

# Calculate and display key financial ratios
def calculate_telco_ratios():
    ratios = {}
    
    # Profitability ratios
    revenue = get_safe_value(income_df, 'Netto-omzet', current_year)
    operating_result = get_safe_value(income_df, 'Bedrijfsresultaat', current_year)
    net_result = get_safe_value(income_df, 'Resultaat na belastingen', current_year)
    
    if revenue > 0:
        ratios['Operating_Margin'] = (operating_result / revenue) * 100
        ratios['Net_Margin'] = (net_result / revenue) * 100
    
    # Financial position ratios
    total_assets = get_safe_value(balance_df, 'Totaal activa', current_year)
    equity = get_safe_value(balance_df, 'Eigen vermogen', current_year)
    current_liabilities = get_safe_value(balance_df, 'Kortlopende schulden', current_year)
    
    if total_assets > 0:
        ratios['Equity_Ratio'] = (equity / total_assets) * 100
        ratios['Asset_Turnover'] = revenue / total_assets if revenue > 0 else 0
    
    # Liquidity ratios
    cash = get_safe_value(balance_df, 'Liquide middelen', current_year)
    receivables = get_safe_value(balance_df, 'Vorderingen', current_year)
    
    ratios['Working_Capital'] = cash + receivables - current_liabilities
    
    return ratios

ratios = calculate_telco_ratios()

print("### Key Performance Indicators")

if 'Equity_Ratio' in ratios:
    print(f"- **Equity Ratio:** {ratios['Equity_Ratio']:.1f}%")

if 'Working_Capital' in ratios:
    print(f"- **Working Capital:** {format_currency_eur(ratios['Working_Capital'])}")

if 'Operating_Margin' in ratios and not np.isnan(ratios['Operating_Margin']):
    print(f"- **Operating Margin:** {ratios['Operating_Margin']:.1f}%")

if 'Asset_Turnover' in ratios:
    print(f"- **Asset Turnover:** {ratios['Asset_Turnover']:.2f}x")

print()
print("### Financial Strength Assessment")

equity_ratio = ratios.get('Equity_Ratio', 0)
if equity_ratio > 50:
    print("- **Equity Position:** Strong - equity ratio above 50%")
elif equity_ratio > 30:
    print("- **Equity Position:** Adequate - equity ratio between 30-50%")
else:
    print("- **Equity Position:** Needs attention - equity ratio below 30%")

working_capital = ratios.get('Working_Capital', 0)
if working_capital > 100000:
    print("- **Liquidity:** Strong - sufficient working capital")
elif working_capital > 0:
    print("- **Liquidity:** Adequate - positive working capital")
else:
    print("- **Liquidity:** Tight - negative working capital")
