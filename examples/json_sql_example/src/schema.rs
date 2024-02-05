use diesel::table;

table! {
    items (id) {
        id -> Int4,
        name -> Varchar,
        description -> Text,
    }
}
