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

class QueryNode : public ParserNode {
public:
    ~QueryNode() override;
    virtual Query visit(ParserDriver*) = 0;
    virtual void canonicalize() {}
};

class LogicalNode : public QueryNode {
public:
    std::vector<QueryNode*> children;
    LogicalNode(QueryNode* left, QueryNode* right)
    {
        children.emplace_back(left);
        children.emplace_back(right);
    }
    void canonicalize() override
    {
        std::vector<QueryNode*> newChildren;
        auto& my_type = typeid(*this);
        for (auto& child : children) {
            child->canonicalize();
            if (typeid(*child) == my_type) {
                auto logical_node = static_cast<LogicalNode*>(child);
                for (auto c : logical_node->children) {
                    newChildren.push_back(c);
                }
            }
            else {
                newChildren.push_back(child);
            }
        }
        children = newChildren;
    }

private:
    virtual std::string get_operator() const = 0;
};

class AndNode : public LogicalNode {
public:
    using LogicalNode::LogicalNode;
    Query visit(ParserDriver*) override;

private:
    std::string get_operator() const override
    {
        return " && ";
    }
};

class OrNode : public LogicalNode {
public:
    using LogicalNode::LogicalNode;
    Query visit(ParserDriver*) override;

private:
    std::string get_operator() const override
    {
        return " || ";
    }
};

class NotNode : public QueryNode {
public:
    QueryNode* query = nullptr;

    NotNode(QueryNode* q)
        : query(q)
    {
    }
    Query visit(ParserDriver*) override;
};

class CompareNode : public QueryNode {
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
        TYPED_LINK,
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

class ListNode : public ParserNode {
public:
    std::vector<ConstantNode*> elements;

    ListNode(ConstantNode* elem)
    {
        elements.emplace_back(elem);
    }

    void add_element(ConstantNode* elem)
    {
        elements.emplace_back(elem);
    }
};

class PropertyNode : public ParserNode {
public:
    virtual std::unique_ptr<Subexpr> visit(ParserDriver*) = 0;
};

class ExpressionNode : public ParserNode {
public:
    virtual bool is_constant()
    {
        return false;
    }
    virtual std::unique_ptr<Subexpr> visit(ParserDriver*, DataType = type_Int) = 0;
};

class ValueNode : public ExpressionNode {
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
    bool is_constant() final
    {
        return constant != nullptr;
    }
    ConstantNode* get_constant()
    {
        REALM_ASSERT(is_constant());
        return constant;
    }

    std::unique_ptr<Subexpr> visit(ParserDriver* drv, DataType type) override
    {
        if (prop)
            return prop->visit(drv);
        return constant->visit(drv, type);
    }
};

class OperationNode : public ExpressionNode {
public:
    ExpressionNode* m_left;
    ExpressionNode* m_right;
    char m_op;
    OperationNode(ExpressionNode* left, char op, ExpressionNode* right)
        : m_left(left)
        , m_right(right)
        , m_op(op)
    {
    }
    bool is_constant() final
    {
        return m_left->is_constant() && m_right->is_constant();
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*, DataType) override;
};

class EqualityNode : public CompareNode {
public:
    std::vector<ExpressionNode*> values;
    int op;
    bool case_sensitive = true;

    EqualityNode(ExpressionNode* left, int t, ExpressionNode* right)
        : op(t)
    {
        values.emplace_back(left);
        values.emplace_back(right);
    }
    Query visit(ParserDriver*) override;
};

class RelationalNode : public CompareNode {
public:
    std::vector<ExpressionNode*> values;
    int op;

    RelationalNode(ExpressionNode* left, int t, ExpressionNode* right)
        : op(t)
    {
        values.emplace_back(left);
        values.emplace_back(right);
    }
    Query visit(ParserDriver*) override;
};

class BetweenNode : public CompareNode {
public:
    ValueNode* prop;
    ListNode* limits;

    BetweenNode(ValueNode* left, ListNode* right)
        : prop(left)
        , limits(right)
    {
    }
    Query visit(ParserDriver*) override;
};

class StringOpsNode : public CompareNode {
public:
    std::vector<ExpressionNode*> values;
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

class TrueOrFalseNode : public QueryNode {
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
    ConstantNode* index = nullptr;

    PropNode(PathNode* node, std::string id, ConstantNode* idx, PostOpNode* po_node)
        : path(node)
        , identifier(id)
        , post_op(po_node)
        , index(idx)
    {
    }
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
    QueryNode* subquery = nullptr;

    SubqueryNode(PropNode* node, std::string var_name, QueryNode* query)
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
    using SubexprPtr = std::unique_ptr<Subexpr>;
    class ParserNodeStore {
    public:
        template <typename T, typename... Args>
        T* create(Args&&... args)
        {
            auto owned = std::make_unique<T>(std::forward<Args>(args)...);
            auto ret = owned.get();
            m_store.push_back(std::move(owned));
            return ret;
        }

    private:
        std::vector<std::unique_ptr<ParserNode>> m_store;
    };

    ParserDriver()
        : ParserDriver(TableRef(), s_default_args, s_default_mapping)
    {
    }

    ParserDriver(TableRef t, Arguments& args, const query_parser::KeyPathMapping& mapping);
    ~ParserDriver();

    util::serializer::SerialisationState m_serializer_state;
    QueryNode* result = nullptr;
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

    StringData get_printable_name(StringData table_name) const;

    template <class T>
    Query simple_query(int op, ColKey col_key, T val, bool case_sensitive);
    template <class T>
    Query simple_query(int op, ColKey col_key, T val);
    std::pair<SubexprPtr, SubexprPtr> cmp(const std::vector<ExpressionNode*>& values);
    SubexprPtr column(LinkChain&, std::string);
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
