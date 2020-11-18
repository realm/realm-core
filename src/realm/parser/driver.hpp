#ifndef DRIVER_HH
#define DRIVER_HH
#include <string>
#include <map>
#include <external/mpark/variant.hpp>
#include "realm/parser/query_bison.hpp"
#include "realm/parser/query_parser.hpp"
#include "realm/util/any.hpp"

// Give Flex the prototype of yylex we want ...
#define YY_DECL yy::parser::symbol_type yylex(realm::query_parser::ParserDriver&)
// ... and declare it for the parser's sake.
YY_DECL;

#undef FALSE
#undef TRUE

namespace realm {

namespace query_parser {

class ParserNode {
public:
    virtual ~ParserNode();
    virtual util::Any visit(ParserDriver*) = 0;
};

class OrNode : public ParserNode {
public:
    std::vector<AndNode*> and_preds;

    OrNode(AndNode* node)
    {
        and_preds.emplace_back(node);
    }
    util::Any visit(ParserDriver*) override;
};

class AndNode : public ParserNode {
public:
    std::vector<AtomPredNode*> atom_preds;

    AndNode(AtomPredNode* node)
    {
        atom_preds.emplace_back(node);
    }
    util::Any visit(ParserDriver*) override;
};

class AtomPredNode : public ParserNode {
public:
    ~AtomPredNode() override;
};

class NotNode : public AtomPredNode {
public:
    AtomPredNode* atom_pred = nullptr;

    NotNode(AtomPredNode* expr)
        : atom_pred(expr)
    {
    }
    util::Any visit(ParserDriver*) override;
};

class ParensNode : public AtomPredNode {
public:
    OrNode* pred = nullptr;

    ParensNode(OrNode* expr)
        : pred(expr)
    {
    }
    util::Any visit(ParserDriver*) override;
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
};

class EqualitylNode : public CompareNode {
public:
    std::vector<ValueNode*> values;
    int op;
    bool case_sensitive = true;

    EqualitylNode(ValueNode* left, int t, ValueNode* right)
        : op(t)
    {
        values.emplace_back(left);
        values.emplace_back(right);
    }
    util::Any visit(ParserDriver*) override;
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
    util::Any visit(ParserDriver*) override;
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
    util::Any visit(ParserDriver*) override;
};

class ConstantNode : public ParserNode {
public:
    enum Type { NUMBER, INFINITY_VAL, NAN_VAL, FLOAT, STRING, TIMESTAMP, UUID_T, OID, NULL_VAL, TRUE, FALSE, ARG };

    Type type;
    std::string text;

    ConstantNode(Type t, const std::string& str)
        : type(t)
        , text(str)
    {
    }
    util::Any visit(ParserDriver*) override;
};

class PostOpNode : public ParserNode {
public:
    enum Type { COUNT, SIZE };

    Type type;

    PostOpNode(Type t)
        : type(t)
    {
    }
    util::Any visit(ParserDriver*) override;
};

class AggrNode : public ParserNode {
public:
    enum Type { MAX, MIN, SUM, AVG };

    Type type;

    AggrNode(Type t)
        : type(t)
    {
    }
    util::Any visit(ParserDriver*) override;
};

class PropertyNode : public ParserNode {
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
    util::Any visit(ParserDriver*) override;
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
    util::Any visit(ParserDriver*) override;
};

class PropNode : public PropertyNode {
public:
    PathNode* path;
    std::string identifier;
    ExpressionComparisonType comp_type = ExpressionComparisonType::Any;
    PostOpNode* post_op = nullptr;

    PropNode(PathNode* node, std::string id, ExpressionComparisonType ct)
        : path(node)
        , identifier(id)
        , comp_type(ct)
    {
    }
    PropNode(PathNode* node, std::string id, PostOpNode* po_node)
        : path(node)
        , identifier(id)
        , post_op(po_node)
    {
    }
    util::Any visit(ParserDriver*) override;
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
    util::Any visit(ParserDriver*) override;
};

class TrueOrFalseNode : public AtomPredNode {
public:
    bool true_or_false;

    TrueOrFalseNode(bool type)
        : true_or_false(type)
    {
    }
    util::Any visit(ParserDriver*) override;
};


class PathNode : public ParserNode {
public:
    util::Any visit(ParserDriver*) override;

    std::vector<std::string> path_elems;
};

// Conducting the whole scanning and parsing of Calc++.
class ParserDriver {
public:
    using Values = mpark::variant<ExpressionComparisonType, Subexpr*, DataType>;
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
        : m_args(s_default_args)
    {
    }

    ParserDriver(TableRef t, Arguments& args)
        : m_base_table(t)
        , m_args(args)
    {
    }

    ParserNode* result = nullptr;
    TableRef m_base_table;
    Arguments& m_args;
    ParserNodeStore m_parse_nodes;

    // Run the parser on file F.  Return 0 on success.
    int parse(const std::string& str);

    // Handling the scanner.
    void scan_begin(bool trace_scanning);
    void scan_end();

    void error(const std::string& err)
    {
        error_string = err;
        parse_error = true;
    }

    void push(const Values& val)
    {
        m_values.emplace_back(val);
    }
    Values pop()
    {
        REALM_ASSERT(!m_values.empty());
        auto ret = m_values.back();
        m_values.pop_back();
        return ret;
    }

    template <class T>
    Query simple_query(int op, ColKey col_key, T val);
    std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>> cmp(const std::vector<ValueNode*>& values);

private:
    // The string being parsed.
    std::string parse_string;
    std::string error_string;
    void* scan_buffer = nullptr;
    bool parse_error = false;
    std::vector<Values> m_values;

    static NoArguments s_default_args;
};

template <class T>
Query ParserDriver::simple_query(int op, ColKey col_key, T val)
{
    switch (op) {
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

} // namespace query_parser
} // namespace realm
#endif // ! DRIVER_HH
