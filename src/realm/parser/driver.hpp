#ifndef DRIVER_HH
#define DRIVER_HH
#include <string>
#include <map>

#include "realm/query_expression.hpp"
#include "realm/parser/keypath_mapping.hpp"
#include "realm/parser/query_parser.hpp"

#define YY_DECL yy::parser::symbol_type yylex(void* yyscanner)
#include "realm/parser/generated/query_bison.hpp"
YY_DECL;

#undef FALSE
#undef TRUE
#undef IN

namespace realm {

namespace query_parser {

class ParserNode {
public:
    virtual ~ParserNode();
};

class AtomPredNode : public ParserNode {
public:
    ~AtomPredNode() override;
    virtual Query visit(ParserDriver*) = 0;
};

class AndNode : public ParserNode {
public:
    std::vector<AtomPredNode*> atom_preds;

    AndNode(AtomPredNode* node)
    {
        atom_preds.emplace_back(node);
    }
    Query visit(ParserDriver*);
};

class OrNode : public ParserNode {
public:
    std::vector<AndNode*> and_preds;

    OrNode(AndNode* node)
    {
        and_preds.emplace_back(node);
    }
    Query visit(ParserDriver*);
};

class NotNode : public AtomPredNode {
public:
    AtomPredNode* atom_pred = nullptr;

    NotNode(AtomPredNode* expr)
        : atom_pred(expr)
    {
    }
    Query visit(ParserDriver*) override;
};

class ParensNode : public AtomPredNode {
public:
    OrNode* pred = nullptr;

    ParensNode(OrNode* expr)
        : pred(expr)
    {
    }
    Query visit(ParserDriver*) override;
};

class CompareNode : public AtomPredNode {
public:
    static constexpr int EQUAL = 0;
    static constexpr int NOT_EQUAL = 1;
    static constexpr int GREATER = 2;
    static constexpr int LESS = 3;
    static constexpr int GREATER_EQUAL = 4;
    static constexpr int LESS_EQUAL = 5;
    static constexpr int BEGINSWITH = 6;
    static constexpr int ENDSWITH = 7;
    static constexpr int CONTAINS = 8;
    static constexpr int LIKE = 9;
    static constexpr int IN = 10;
};

class ConstantNode : public ParserNode {
public:
    enum Type {
        NUMBER,
        INFINITY_VAL,
        NAN_VAL,
        FLOAT,
        STRING,
        BASE64,
        TIMESTAMP,
        UUID_T,
        OID,
        LINK,
        NULL_VAL,
        TRUE,
        FALSE,
        ARG
    };

    Type type;
    std::string text;

    ConstantNode(Type t, const std::string& str)
        : type(t)
        , text(str)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*, DataType);
};

class PropertyNode : public ParserNode {
public:
    virtual std::unique_ptr<Subexpr> visit(ParserDriver*) = 0;
};

class ValueNode : public ParserNode {
public:
    ConstantNode* constant = nullptr;
    PropertyNode* prop = nullptr;

    ValueNode(ConstantNode* node)
        : constant(node)
    {
    }
    ValueNode(PropertyNode* node)
        : prop(node)
    {
    }
};

class EqualityNode : public CompareNode {
public:
    std::vector<ValueNode*> values;
    int op;
    bool case_sensitive = true;

    EqualityNode(ValueNode* left, int t, ValueNode* right)
        : op(t)
    {
        values.emplace_back(left);
        values.emplace_back(right);
    }
    Query visit(ParserDriver*) override;
};

class RelationalNode : public CompareNode {
public:
    std::vector<ValueNode*> values;
    int op;

    RelationalNode(ValueNode* left, int t, ValueNode* right)
        : op(t)
    {
        values.emplace_back(left);
        values.emplace_back(right);
    }
    Query visit(ParserDriver*) override;
};

class StringOpsNode : public CompareNode {
public:
    std::vector<ValueNode*> values;
    int op;
    bool case_sensitive = true;

    StringOpsNode(ValueNode* left, int t, ValueNode* right)
        : op(t)
    {
        values.emplace_back(left);
        values.emplace_back(right);
    }
    Query visit(ParserDriver*) override;
};

class TrueOrFalseNode : public AtomPredNode {
public:
    bool true_or_false;

    TrueOrFalseNode(bool type)
        : true_or_false(type)
    {
    }
    Query visit(ParserDriver*);
};

class PostOpNode : public ParserNode {
public:
    enum OpType { SIZE, TYPE } op_type;
    std::string op_name;

    PostOpNode(std::string op_literal, OpType type)
        : op_type(type)
        , op_name(op_literal)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*, Subexpr* subexpr);
};

class AggrNode : public ParserNode {
public:
    enum Type { MAX, MIN, SUM, AVG };

    Type type;

    AggrNode(Type t)
        : type(t)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*, Subexpr* subexpr);
};

class PathNode : public ParserNode {
public:
    std::vector<std::string> path_elems;

    LinkChain visit(ParserDriver*, ExpressionComparisonType = ExpressionComparisonType::Any);
    void add_element(const std::string& str)
    {
        path_elems.push_back(str);
    }
};

class ListAggrNode : public PropertyNode {
public:
    PathNode* path;
    std::string identifier;
    AggrNode* aggr_op;

    ListAggrNode(PathNode* node, std::string id, AggrNode* aggr)
        : path(node)
        , identifier(id)
        , aggr_op(aggr)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
};

class LinkAggrNode : public PropertyNode {
public:
    PathNode* path;
    std::string link;
    AggrNode* aggr_op;
    std::string prop;

    LinkAggrNode(PathNode* node, std::string id1, AggrNode* aggr, std::string id2)
        : path(node)
        , link(id1)
        , aggr_op(aggr)
        , prop(id2)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
};

class PropNode : public PropertyNode {
public:
    PathNode* path;
    std::string identifier;
    ExpressionComparisonType comp_type = ExpressionComparisonType::Any;
    PostOpNode* post_op = nullptr;

    PropNode(PathNode* node, std::string id, PostOpNode* po_node,
             ExpressionComparisonType ct = ExpressionComparisonType::Any)
        : path(node)
        , identifier(id)
        , comp_type(ct)
        , post_op(po_node)
    {
    }
    PropNode(PathNode* node, std::string id)
        : path(node)
        , identifier(id)
        , comp_type(ExpressionComparisonType::Any)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
};

class SubqueryNode : public PropertyNode {
public:
    PropNode* prop = nullptr;
    std::string variable_name;
    OrNode* subquery = nullptr;

    SubqueryNode(PropNode* node, std::string var_name, OrNode* query)
        : prop(node)
        , variable_name(var_name)
        , subquery(query)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
};

class DescriptorNode : public ParserNode {
public:
    enum Type { SORT, DISTINCT, LIMIT };
    std::vector<std::vector<std::string>> columns;
    std::vector<bool> ascending;
    size_t limit = size_t(-1);
    Type type;

    DescriptorNode(Type t)
        : type(t)
    {
    }
    DescriptorNode(Type t, const std::string& str)
        : type(t)
    {
        limit = size_t(strtol(str.c_str(), nullptr, 0));
    }
    ~DescriptorNode() override;
    Type get_type()
    {
        return type;
    }
    void add(const std::vector<std::string>& path, const std::string& id)
    {
        columns.push_back(path);
        columns.back().push_back(id);
    }
    void add(const std::vector<std::string>& path, const std::string& id, bool direction)
    {
        add(path, id);
        ascending.push_back(direction);
    }
};

class DescriptorOrderingNode : public ParserNode {
public:
    std::vector<DescriptorNode*> orderings;

    DescriptorOrderingNode() = default;
    ~DescriptorOrderingNode() override;
    void add_descriptor(DescriptorNode* n)
    {
        orderings.push_back(n);
    }
    std::unique_ptr<DescriptorOrdering> visit(ParserDriver* drv);
};

// Conducting the whole scanning and parsing of Calc++.
class ParserDriver {
public:
    class ParserNodeStore {
    public:
        template <typename T, typename... Args>
        T* create(Args&&... args)
        {
            auto ret = new T(args...);
            m_store.push_back(ret);
            return ret;
        }

        ~ParserNodeStore()
        {
            for (auto it : m_store) {
                delete it;
            }
        }

    private:
        std::vector<ParserNode*> m_store;
    };

    ParserDriver()
        : ParserDriver(TableRef(), s_default_args, s_default_mapping)
    {
    }

    ParserDriver(TableRef t, Arguments& args, const query_parser::KeyPathMapping& mapping);
    ~ParserDriver();

    OrNode* result = nullptr;
    DescriptorOrderingNode* ordering = nullptr;
    TableRef m_base_table;
    Arguments& m_args;
    query_parser::KeyPathMapping m_mapping;
    ParserNodeStore m_parse_nodes;
    void* m_yyscanner;

    // Run the parser on file F.  Return 0 on success.
    int parse(const std::string& str);

    // Handling the scanner.
    void scan_begin(void*, bool trace_scanning);

    void error(const std::string& err)
    {
        error_string = err;
        parse_error = true;
    }

    template <class T>
    Query simple_query(int op, ColKey col_key, T val, bool case_sensitive);
    template <class T>
    Query simple_query(int op, ColKey col_key, T val);
    std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>> cmp(const std::vector<ValueNode*>& values);
    Subexpr* column(LinkChain&, std::string);
    void backlink(LinkChain&, const std::string&);
    std::string translate(LinkChain&, const std::string&);

private:
    // The string being parsed.
    util::StringBuffer parse_buffer;
    std::string error_string;
    void* scan_buffer = nullptr;
    bool parse_error = false;

    static NoArguments s_default_args;
    static query_parser::KeyPathMapping s_default_mapping;
};

template <class T>
Query ParserDriver::simple_query(int op, ColKey col_key, T val, bool case_sensitive)
{
    switch (op) {
        case CompareNode::IN:
        case CompareNode::EQUAL:
            return m_base_table->where().equal(col_key, val, case_sensitive);
        case CompareNode::NOT_EQUAL:
            return m_base_table->where().not_equal(col_key, val, case_sensitive);
    }
    return m_base_table->where();
}

template <class T>
Query ParserDriver::simple_query(int op, ColKey col_key, T val)
{
    switch (op) {
        case CompareNode::IN:
        case CompareNode::EQUAL:
            return m_base_table->where().equal(col_key, val);
        case CompareNode::NOT_EQUAL:
            return m_base_table->where().not_equal(col_key, val);
        case CompareNode::GREATER:
            return m_base_table->where().greater(col_key, val);
        case CompareNode::LESS:
            return m_base_table->where().less(col_key, val);
        case CompareNode::GREATER_EQUAL:
            return m_base_table->where().greater_equal(col_key, val);
        case CompareNode::LESS_EQUAL:
            return m_base_table->where().less_equal(col_key, val);
    }
    return m_base_table->where();
}

std::string check_escapes(const char* str);

} // namespace query_parser
} // namespace realm
#endif // ! DRIVER_HH
