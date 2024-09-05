def percentage_to_us_odds(percentage):
    if percentage >= 0.50:
        odds = -(percentage / (1.00 - percentage)) * 100
    else:
        odds = ((1.00 - percentage) / percentage) * 100
    return round(odds)
