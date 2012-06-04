TABLE_MACROS_H="$1"

if python table_macros-gen.py 8 >/tmp/table_macros.hpp; then
    mv /tmp/table_macros.hpp "$TABLE_MACROS_H"
else
    if [ -e "$TABLE_MACROS_H" ]; then
        echo "WARNING: Failed to update '$TABLE_MACROS_H'"
    else
        exit 1
    fi
fi
