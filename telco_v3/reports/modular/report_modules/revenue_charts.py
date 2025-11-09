# Telco B.V. Revenue Charts Module

print("## Revenue Analysis")
print()

# Extract revenue data
revenue_current = get_safe_value(income_df, 'Netto-omzet', current_year)
revenue_previous = get_safe_value(income_df, 'Netto-omzet', previous_year)

if revenue_current > 0 or revenue_previous > 0:
    # Revenue trend chart
    fig = go.Figure()
    
    years = [previous_year, current_year]
    revenues = [revenue_previous, revenue_current]
    
    fig.add_trace(go.Bar(
        x=years,
        y=revenues,
        name='Revenue',
        marker_color=['#1f77b4', '#ff7f0e'],
        text=[format_currency_eur(rev) for rev in revenues],
        textposition='auto',
        textfont=dict(size=12, color='white')
    ))
    
    fig.update_layout(
        title="Revenue Development",
        xaxis_title="Year",
        yaxis_title="Revenue (EUR)",
        showlegend=False,
        height=400,
        plot_bgcolor='white',
        yaxis=dict(gridcolor='lightgray')
    )
    
    fig.show()
    
    # Revenue growth analysis
    if revenue_previous > 0 and revenue_current > revenue_previous:
        growth_rate = ((revenue_current - revenue_previous) / revenue_previous) * 100
        print(f"Revenue grew by {growth_rate:.1f}% from {previous_year} to {current_year}.")
    elif revenue_previous == 0 and revenue_current > 0:
        print(f"New revenue streams established in {current_year}.")
    elif revenue_current < revenue_previous:
        decline_rate = ((revenue_previous - revenue_current) / revenue_previous) * 100
        print(f"Revenue decreased by {decline_rate:.1f}% from {previous_year} to {current_year}.")
    
else:
    print("No significant revenue to analyze for the current period.")
