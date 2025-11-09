# Telco B.V. Executive Summary Module

print("## Telco B.V. - Financial Highlights")
print()

company_name = metadata.get('company_name', 'Telco B.V.')
report_date = f"December 31, {current_year}"

# Extract key financial metrics
revenue_current = get_safe_value(income_df, 'Netto-omzet', current_year)
revenue_previous = get_safe_value(income_df, 'Netto-omzet', previous_year)

operating_result_current = get_safe_value(income_df, 'Bedrijfsresultaat', current_year)
net_result_current = get_safe_value(income_df, 'Resultaat na belastingen', current_year)

total_assets_current = get_safe_value(balance_df, 'Totaal activa', current_year)
equity_current = get_safe_value(balance_df, 'Eigen vermogen', current_year)

# Calculate key ratios
equity_ratio = (equity_current / total_assets_current * 100) if total_assets_current > 0 else 0

print(f"**Company:** {company_name}")
print(f"**Reporting Period:** Year ended {report_date}")
print()

print("### Financial Performance")
print(f"- **Revenue:** {format_currency_eur(revenue_current)} ({current_year}) vs {format_currency_eur(revenue_previous)} ({previous_year})")

if revenue_current > revenue_previous and revenue_previous > 0:
    growth = ((revenue_current - revenue_previous) / revenue_previous * 100)
    print(f"- **Revenue Growth:** {growth:.1f}%")
elif revenue_current > 0 and revenue_previous == 0:
    print(f"- **Revenue Growth:** New revenue stream established")
else:
    print(f"- **Revenue Change:** Decreased from previous year")

print(f"- **Operating Result:** {format_currency_eur(operating_result_current)}")
print(f"- **Net Result:** {format_currency_eur(net_result_current)}")
print()

print("### Financial Position")
print(f"- **Total Assets:** {format_currency_eur(total_assets_current)}")
print(f"- **Equity:** {format_currency_eur(equity_current)}")
print(f"- **Equity Ratio:** {equity_ratio:.1f}%")
print()

print("### Key Developments in 2024")
real_estate_inv = get_safe_value(balance_df, 'Vastgoedbeleggingen', current_year)
if real_estate_inv > 0:
    print(f"- Investment in real estate: {format_currency_eur(real_estate_inv)}")

fin_assets_current = get_safe_value(balance_df, 'Financiële vaste activa', current_year)
fin_assets_previous = get_safe_value(balance_df, 'Financiële vaste activa', previous_year)
if fin_assets_current < fin_assets_previous:
    decrease = fin_assets_previous - fin_assets_current
    print(f"- Reduction in financial assets: {format_currency_eur(decrease)}")

print("- Continued focus on investment activities and financial management")
