
def print_to_terminal(weekly_update: str, week_start_date: str) -> None:

    title = "| Weekly Sales Update for {} |".format(week_start_date)

    print(f"\n\n{'-' * ((68-len(title))//2)}{title}{'-' * ((68-len(title))//2)}\n\n")
    print(weekly_update)
    print(f"\n\n{'-' * 30}| End |{'-' * 30}\n\n")