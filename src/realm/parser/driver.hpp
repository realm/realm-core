#ifndef DRIVER_HH
#define DRIVER_HH
#include <string>
#include <map>
#include <external/json/json.hpp>

#include "realm/query_expression.hpp"
#include "realm/parser/keypath_mapping.hpp"
#include "realm/parser/query_parser.hpp"
#include "realm/sort_descriptor.hpp"

#define YY_DECL yy::parser::symbol_type yylex(void* yyscanner)
#include "realm/parser/generated/query_bison.hpp"
YY_DECL;

#undef FALSE
#undef TRUE
#undef IN

namespace realm {

namespace query_parser {

class NodeVisitor;

class ParserNode {  
public:
    virtual ~ParserNode();
    virtual void accept(NodeVisitor& visitor) = 0;
};

class AtomPredNode : public ParserNode {
public:
    ~AtomPredNode() override;
    virtual Query visit(ParserDriver*) = 0;
};

class AndNode : public ParserNode {
public:
    std::vector<std::unique_ptr<AtomPredNode>> atom_preds;

    AndNode(std::unique_ptr<AtomPredNode>&& node)
    {
        atom_preds.emplace_back(std::move(node));
    }
    Query visit(ParserDriver*);
    void accept(NodeVisitor& visitor) override;
};

class OrNode : public ParserNode {
public:
    std::vector<std::unique_ptr<AndNode>> and_preds;

    OrNode(std::unique_ptr<AndNode>&& node)
    {
        and_preds.emplace_back(std::move(node));
    }
    Query visit(ParserDriver*);
    void accept(NodeVisitor& visitor) override;
};

class NotNode : public AtomPredNode {
public:
    std::unique_ptr<AtomPredNode> atom_pred = nullptr;

    NotNode(std::unique_ptr<AtomPredNode>&& expr)
        : atom_pred(std::move(expr))
    {
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class ParensNode : public AtomPredNode {
public:
    std::unique_ptr<OrNode> pred = nullptr;

    ParensNode(std::unique_ptr<OrNode>&& expr)
        : pred(std::move(expr))
    {
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
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

class ValueNode : public ParserNode {
public:
    std::unique_ptr<ConstantNode> constant = nullptr;
    std::unique_ptr<PropertyNode> prop = nullptr;

    ValueNode(std::unique_ptr<ConstantNode>&& node)
        : constant(std::move(node))
    {
    }
    ValueNode(std::unique_ptr<PropertyNode>&& node)
        : prop(std::move(node))
    {
    }
    void accept(NodeVisitor& visitor) override;
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
    void accept(NodeVisitor& visitor) override;
};

class PropertyNode : public ParserNode {
public:
    virtual std::unique_ptr<Subexpr> visit(ParserDriver*) = 0;
};

class EqualityNode : public CompareNode {
public:
    std::vector<std::unique_ptr<ValueNode>> values;
    int op;
    bool case_sensitive = true;

    EqualityNode(std::unique_ptr<ValueNode>&& left, int t, std::unique_ptr<ValueNode>&& right)
        : op(t)
    {
        values.emplace_back(std::move(left));
        values.emplace_back(std::move(right));
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class RelationalNode : public CompareNode {
public:
    std::vector<std::unique_ptr<ValueNode>> values;
    int op;

    RelationalNode(std::unique_ptr<ValueNode>&& left, int t, std::unique_ptr<ValueNode>&& right)
        : op(t)
    {
        values.emplace_back(std::move(left));
        values.emplace_back(std::move(right));
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class StringOpsNode : public CompareNode {
public:
    std::vector<std::unique_ptr<ValueNode>> values;
    int op;
    bool case_sensitive = true;

    StringOpsNode(std::unique_ptr<ValueNode>&& left, int t, std::unique_ptr<ValueNode>&& right)
        : op(t)
    {
        values.emplace_back(std::move(left));
        values.emplace_back(std::move(right));
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class TrueOrFalseNode : public AtomPredNode {
public:
    bool true_or_false;

    TrueOrFalseNode(bool type)
        : true_or_false(type)
    {
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
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
    void accept(NodeVisitor& visitor) override;
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
    void accept(NodeVisitor& visitor) override;
};

class PathNode : public ParserNode {
public:
    std::vector<std::string> path_elems;

    LinkChain visit(ParserDriver*, ExpressionComparisonType = ExpressionComparisonType::Any);
    void add_element(const std::string& str)
    {
        path_elems.push_back(str);
    }
    void accept(NodeVisitor& visitor) override;
};

class ListAggrNode : public PropertyNode {
public:
    std::unique_ptr<PathNode> path;
    std::string identifier;
    std::unique_ptr<AggrNode> aggr_op;

    ListAggrNode(std::unique_ptr<PathNode>&& node, std::string id, std::unique_ptr<AggrNode>&& aggr)
        : path(std::move(node))
        , identifier(id)
        , aggr_op(std::move(aggr))
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class LinkAggrNode : public PropertyNode {
public:
    std::unique_ptr<PathNode> path;
    std::string link;
    std::unique_ptr<AggrNode> aggr_op;
    std::string prop;

    LinkAggrNode(std::unique_ptr<PathNode>&& node, std::string id1, std::unique_ptr<AggrNode>&& aggr, std::string id2)
        : path(std::move(node))
        , link(id1)
        , aggr_op(std::move(aggr))
        , prop(id2)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class PropNode : public PropertyNode {
public:
    std::unique_ptr<PathNode> path;
    std::string identifier;
    ExpressionComparisonType comp_type = ExpressionComparisonType::Any;
    std::unique_ptr<PostOpNode> post_op = nullptr;
    std::unique_ptr<ConstantNode> index = nullptr;

    PropNode(std::unique_ptr<PathNode>&& node, std::string id, std::unique_ptr<ConstantNode> idx, std::unique_ptr<PostOpNode>&& po_node)
        : path(std::move(node))
        , identifier(id)
        , post_op(std::move(po_node))
        , index(std::move(idx))
    {
    }
    PropNode(std::unique_ptr<PathNode>&& node, std::string id, std::unique_ptr<PostOpNode>&& po_node,
             ExpressionComparisonType ct = ExpressionComparisonType::Any)
        : path(std::move(node))
        , identifier(id)
        , comp_type(ct)
        , post_op(std::move(po_node))
    {
    }
    PropNode(std::unique_ptr<PathNode>&& node, std::string id)
        : path(std::move(node))
        , identifier(id)
        , comp_type(ExpressionComparisonType::Any)
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
};

class SubqueryNode : public PropertyNode {
public:
    std::unique_ptr<PropNode> prop = nullptr;
    std::string variable_name;
    std::unique_ptr<OrNode> subquery = nullptr;

    SubqueryNode(std::unique_ptr<PropNode>&& node, std::string var_name, std::unique_ptr<OrNode>&& query)
        : prop(std::move(node))
        , variable_name(var_name)
        , subquery(std::move(query))
    {
    }
    std::unique_ptr<Subexpr> visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) override;
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
    void accept(NodeVisitor& visitor) override;
};

class DescriptorOrderingNode : public ParserNode {
public:
    std::vector<std::unique_ptr<DescriptorNode>> orderings;

    DescriptorOrderingNode() = default;
    ~DescriptorOrderingNode() override;
    void add_descriptor(std::unique_ptr<DescriptorNode>&& n)
    {
        orderings.push_back(std::move(n));
    }
    std::unique_ptr<DescriptorOrdering> visit(ParserDriver* drv);
    void accept(NodeVisitor& visitor) override;
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

    util::serializer::SerialisationState m_serializer_state;
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

    StringData get_printable_name(StringData table_name) const;

    template <class T>
    Query simple_query(int op, ColKey col_key, T val, bool case_sensitive);
    template <class T>
    Query simple_query(int op, ColKey col_key, T val);
    std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>> cmp(std::vector<std::unique_ptr<ValueNode>>&& values);
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

class NodeVisitor {
public:
    virtual void visitAnd(AndNode& and_node);
    virtual void visitOr(OrNode& or_node);
    virtual void visitNot(NotNode& not_node);
    virtual void visitParens(ParensNode& parens_node);
    virtual void visitConstant(ConstantNode& constant_node);
    virtual void visitValue(ValueNode& value_node);
    virtual void visitEquality(EqualityNode& equality_node);
    virtual void visitRelational(RelationalNode& relational_node);
    virtual void visitStringOps(StringOpsNode& string_ops_node);
    virtual void visitTrueOrFalse(TrueOrFalseNode& true_or_false_node);
    virtual void visitPostOp(PostOpNode& post_op_node);
    virtual void visitAggr(AggrNode& aggr_node);
    virtual void visitPath(PathNode& path_node);
    virtual void visitListAggr(ListAggrNode& list_aggr_node);
    virtual void visitLinkAggr(LinkAggrNode& link_aggr_node);
    virtual void visitProp(PropNode& prop_node);
    virtual void visitSubquery(SubqueryNode& sub_query_node);
    virtual void visitDescriptor(DescriptorNode& descriptor_node);
    virtual void visitDescriptorOrdering(DescriptorOrderingNode& descriptor_ordering_node);
};

class PrintingVisitor : public NodeVisitor {
using base = NodeVisitor;
public:
    PrintingVisitor(std::ostream& out) : out(out){}
    void visitAnd(AndNode& and_node) override;
    void visitOr(OrNode& or_node) override;
    void visitNot(NotNode& not_node) override;
    void visitParens(ParensNode& parens_node) override;
    void visitConstant(ConstantNode& constant_node) override;
    void visitEquality(EqualityNode& equality_node) override;
    void visitRelational(RelationalNode& relational_node) override;
    void visitStringOps(StringOpsNode& string_ops_node) override;
    void visitTrueOrFalse(TrueOrFalseNode& true_or_false_node) override;
    void visitPostOp(PostOpNode& post_op_node) override;
    void visitAggr(AggrNode& aggr_node) override;
    void visitPath(PathNode& path_node) override;
    void visitListAggr(ListAggrNode& list_aggr_node) override;
    void visitLinkAggr(LinkAggrNode& link_aggr_node) override;
    void visitProp(PropNode& prop_node) override;
    void visitSubquery(SubqueryNode& sub_query_node) override;
    void visitDescriptor(DescriptorNode& descriptor_node) override;
    void visitDescriptorOrdering(DescriptorOrderingNode& descriptor_ordering_node) override;
private:
    std::ostream& out;
};

class QueryVisitor : public NodeVisitor {
using base = NodeVisitor;
public:
    QueryVisitor(ParserDriver *drv): drv(drv){}
    Query visit(ParserNode& node);
    std::unique_ptr<DescriptorOrdering> getDescriptorOrdering(DescriptorOrderingNode& node);
    realm::Query query;
    std::unique_ptr<DescriptorOrdering> descriptor_ordering;
private:
    void visitAnd(AndNode& and_node) override;
    void visitOr(OrNode& or_node) override;
    void visitNot(NotNode& not_node) override;
    void visitEquality(EqualityNode& equality_node) override;
    void visitRelational(RelationalNode& relational_node) override;
    void visitStringOps(StringOpsNode& string_ops_node) override;
    void visitTrueOrFalse(TrueOrFalseNode& true_or_false_node) override;
    std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>> cmp(std::vector<std::unique_ptr<ValueNode>>&& values);
    ParserDriver* drv;
    realm::LinkChain link;
};


class SubexprVisitor : private NodeVisitor {
using base = NodeVisitor;
public:
    SubexprVisitor(ParserDriver *drv): drv(drv){}
    SubexprVisitor(ParserDriver *drv, DataType t): drv(drv), t(t){}
    std::unique_ptr<realm::Subexpr> visit(ParserNode& node);
private:
    void visitConstant(ConstantNode& constant_node) override;
    void visitPostOp(PostOpNode& post_op_node) override;
    void visitAggr(AggrNode& aggr_node) override;
    void visitListAggr(ListAggrNode& list_aggr_node) override;
    void visitLinkAggr(LinkAggrNode& link_aggr_node) override;
    void visitProp(PropNode& prop_node) override;
    void visitSubquery(SubqueryNode& sub_query_node) override;
    LinkChain getLinkChain(PathNode& node, ExpressionComparisonType comp_type = ExpressionComparisonType::Any);
    std::unique_ptr<realm::Subexpr> subexpr;
    ParserDriver* drv;
    DataType t;
};

} // namespace query_parser

class JsonQueryParser {
public:
    Query query_from_json(TableRef table, nlohmann::json json);
private:
    std::unique_ptr<ParserNode> get_query_node(nlohmann::json json);
    std::unique_ptr<ValueNode> get_subexpr_node(nlohmann::json json);

    // ValueNode& property(std::string name);

    // ValueNode& constant(realm::Mixed value);

    // EqualityNode& equals(ValueNode& left, ValueNode& right);

    // AndNode& and_node(std::initializer_list<AtomPredNode&> predicates);

    // OrNode& or_node(std::initializer_list<AndNode&> predicates);

    std::unique_ptr<ConstantNode> constant_node(realm::Mixed value);
};
} // namespace realm
#endif // ! DRIVER_HH
