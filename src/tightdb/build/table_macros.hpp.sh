DIR="$(dirname "$0")"

TABLE_MACROS_HPP="$1"

TEMP_DIR="$(mktemp -d /tmp/tightdb.codegen.XXXX)" || exit 1
if python "$DIR/table_macros.hpp.py" 15 >"$TEMP_DIR/table_macros.hpp"; then
    mv "$TEMP_DIR/table_macros.hpp" "$TABLE_MACROS_HPP"
else
    cat <<EOI 1>&2
ERROR: Failed to update '$TABLE_MACROS_HPP'

If you are sure that '$TABLE_MACROS_HPP' is already up to date, fix
this with:

    touch $(pwd)/$TABLE_MACROS_HPP  # ONLY IF YOU ARE SURE!

Otherwise, you must install the Python Cheetah package.
EOI
    exit 1
fi
