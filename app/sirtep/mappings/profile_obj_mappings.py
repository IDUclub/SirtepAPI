PROFILE_OBJ_PRIORITY_MAP = {
    4: (43, 29, 50, 51, 52),
    7: (5, 50, 51, 52),
    2: (1, 50, 51, 52),
    6: (10, 28, 30, 50, 51, 52),
    5: (60, 50, 51, 52),
    3: (),
}  # profile-objects type map to optimize schedule for.
# Key is profile id from urban db, value is tuple in ranked order (smaller index means greater priority)
# should be moved to env or to external service in future
