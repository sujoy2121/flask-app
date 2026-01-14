while True:
    confirm = input("Confirm করবেন? (y/n): ").strip().lower()

    if confirm in ["y", "yes"]:
        print("✔ Confirmed")
        break
    elif confirm in ["n", "no"]:
        print("❌ Cancelled")
        break
    else:
        print("⚠ ভুল input, আবার দিন")
