#ifndef DRIVER_HH
#define DRIVER_HH
#include <string>
#include <map>

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
    virtual accept(NodeVisitor& visitor) const = 0;
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
    void accept(NodeVisitor& visitor) const;
};

class OrNode : public ParserNode {
public:
    std::vector<AndNode*> and_preds;

    OrNode(AndNode* node)
    {
        and_preds.emplace_back(node);
    }
    Query visit(ParserDriver*);
    void accept(NodeVisitor& visitor) const;
};

class NotNode : public AtomPredNode {
public:
    AtomPredNode* atom_pred = nullptr;

    NotNode(AtomPredNode* expr)
        : atom_pred(expr)
    {
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) const override;
};

class ParensNode : public AtomPredNode {
public:
    OrNode* pred = nullptr;

    ParensNode(OrNode* expr)
        : pred(expr)
    {
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) const override;
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
    virtual DataType get_type() const = 0;
};

class ConstantNode2 : public ValueNode {
public:
    ConstantNode2(Mixed value) : value(value) {

    }

    const Mixed value;

    DataType get_type() const override {
        return value.get_type();
    }
}

class QueryParserConstantNode : public ConstantNode2 {
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

    QueryParserConstantNode(const std::string& text, Type type)
        : ConstantNode2(Mixed(text))
        , type(type) {

    }

    Mixed reduce(DataType hint);

    const Type type;
}

// strings.length == "foobar"
// EqualityNode(Property(Property('strings'), 'length'), QueryParserConstant('2', NUMBER))
// EqualityNode(SizeOf(Property('strings'), Constant(2)))

class QueryParserTranslatingVisitor : public NodeVisitor {
    void visitEquality(EqualityNode& equality) override {
        std::pair<ValueNode&, ValueNode&> [left, right] = cmp(equality.left, equality.right);

        equality = EqualityNode(left, right);

    }

    std::pair<ValueNode&, ValueNode&> cmp(const ValueNode& left, const ValueNode& right) {
        if (right is QueryParserConstantNode) {
            return [left, ConstantNode2(right.reduce(left.get_type()))];
        }
    }
}

class QueryVisitor2 : public NodeVisitor {
    void visitEquality(const EqualityNode& equality) override {
        auto right = equality.right->to_subexpr();
        auto left = equality.left->to_subexpr();
        query = Query(std::unique_ptr<Expression>(new Compare<Equal>(std::move(right), std::move(left))));
    }
}

class ConstantNode : public ValueNode {
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
    void accept(NodeVisitor& visitor) const;
};

class PropertyNode : public ValueNode {
public:
    virtual std::unique_ptr<Subexpr> visit(ParserDriver*) = 0;
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
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const override;
};

class TrueOrFalseNode : public AtomPredNode {
public:
    bool true_or_false;

    TrueOrFalseNode(bool type)
        : true_or_false(type)
    {
    }
    Query visit(ParserDriver*) override;
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const;
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
    void accept(NodeVisitor& visitor) const;
};

class PathNode : public ParserNode {
public:
    std::vector<std::string> path_elems;

    LinkChain visit(ParserDriver*, ExpressionComparisonType = ExpressionComparisonType::Any);
    void add_element(const std::string& str)
    {
        path_elems.push_back(str);
    }
    void accept(NodeVisitor& visitor) const;
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
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const override;
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
    void accept(NodeVisitor& visitor) const;
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
    void accept(NodeVisitor& visitor) const;
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

class NodeVisitor {
public:
    virtual void visitAnd(const AndNode& and_node);
    virtual void visitOr(const OrNode& or_node);
    virtual void visitNot(const NotNode& not_node);
    virtual void visitParens(const ParensNode& parens_node);
    virtual void visitCompare(const CompareNode& compare_node);
    virtual void visitConstant(const ConstantNode& constant_node);
    virtual void visitValue(const ValueNode& value_node);
    virtual void visitEquality(const EqualityNode& equality_node);
    virtual void visitRelational(const RelationalNode& relational_node);
    virtual void visitStringOps(const StringOpsNode& string_ops_node);
    virtual void visitTrueOrFalse(const TrueOrFalseNode& true_or_false_node);
    virtual void visitPostOp(const PostOpNode& post_op_node);
    virtual void visitAggr(const AggrNode& aggr_node);
    virtual void visitPath(const PathNode& path_node);
    virtual void visitListAggr(const ListAggrNode& list_aggr_node);
    virtual void visitLinkAggr(const LinkAggrNode& link_aggr_node);
    virtual void visitProp(const PropNode& prop_node);
    virtual void visitSubquery(const SubqueryNode& sub_query_node);
    virtual void visitDescriptor(const DescriptorNode& descriptor_node);
    virtual void visitDescriptorOrdering(const DescriptorOrderingNode& descriptor_ordering_node);
};

class PrintingVisitor : public NodeVisitor {
using base = NodeVisitor;
public:
    PrintingVisitor(std::ostream& out) : out(out){}
    void visitAnd(const AndNode& and_node) override;
    void visitOr(const OrNode& or_node) override;
    void visitNot(const NotNode& not_node) override;
    void visitParens(const ParensNode& parens_node) override;
    void visitCompare(const CompareNode& compare_node) override;
    void visitConstant(const ConstantNode& constant_node) override;
    void visitValue(const ValueNode& value_node) override;
    void visitEquality(const EqualityNode& equality_node) override;
    void visitRelational(const RelationalNode& relational_node) override;
    void visitStringOps(const StringOpsNode& string_ops_node) override;
    void visitTrueOrFalse(const TrueOrFalseNode& true_or_false_node) override;
    void visitPostOp(const PostOpNode& post_op_node) override;
    void visitAggr(const AggrNode& aggr_node) override;
    void visitPath(const PathNode& path_node) override;
    void visitListAggr(const ListAggrNode& list_aggr_node) override;
    void visitLinkAggr(const LinkAggrNode& link_aggr_node) override;
    void visitProp(const PropNode& prop_node) override;
    void visitSubquery(const SubqueryNode& sub_query_node) override;
    void visitDescriptor(const DescriptorNode& descriptor_node) override;
    void visitDescriptorOrdering(const DescriptorOrderingNode& descriptor_ordering_node) override;
private:
    std::ostream& out;
};

class QueryVisitor : public NodeVisitor {
using base = NodeVisitor;
public:
    QueryVisitor(ParserDriver *drv): drv(drv){}
    Query visit(ParserNode& node);
    realm::Query query;
private:
    void visitAnd(const AndNode& and_node) override;
    void visitOr(const OrNode& or_node) override;
    void visitNot(const NotNode& not_node) override;
    void visitEquality(const EqualityNode& equality_node) override;
    void visitRelational(const RelationalNode& relational_node) override;
    void visitStringOps(const StringOpsNode& string_ops_node) override;
    void visitTrueOrFalse(const TrueOrFalseNode& true_or_false_node) override;
    std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>> cmp(const std::vector<ValueNode*>& values);
    std::unique_ptr<realm::Subexpr>subexpr;
    ParserDriver* drv;
    realm::LinkChain link;
};


class SubexprVisitor : private NodeVisitor {
using base = NodeVisitor;
public:
    SubexprVisitor(ParserDriver *drv): drv(drv){}
    SubexprVisitor(ParserDriver *drv, DataType t): drv(drv), t(t){}
    std::unique_ptr<realm::Subexpr> visit(const ParserNode* node);
private:
    void visitConstant(const ConstantNode& constant_node) override;
    void visitPostOp(const PostOpNode& post_op_node) override;
    void visitAggr(const AggrNode& aggr_node) override;
    void visitListAggr(const ListAggrNode& list_aggr_node) override;
    void visitLinkAggr(const LinkAggrNode& link_aggr_node) override;
    void visitProp(const PropNode& prop_node) override;
    void visitSubquery(const SubqueryNode& sub_query_node) override;
    std::unique_ptr<realm::Subexpr> subexpr;
    ParserDriver* drv;
    DataType t;
};

class LinkChainVisitor : public NodeVisitor {
using base = NodeVisitor;
public:
    LinkChain visit(const PathNode* node);
    LinkChainVisitor(ParserDriver *drv, ExpressionComparisonType comp_type): drv(drv), comp_type(comp_type){}
    LinkChainVisitor(ParserDriver *drv): drv(drv), comp_type(ExpressionComparisonType::Any){}
private:
    void visitPath(const PathNode& path_node) override;
    LinkChain link_chain;
    ParserDriver* drv;
    ExpressionComparisonType comp_type;
};

} // namespace query_parser
} // namespace realm
#endif // ! DRIVER_HH

// class JsonParser2 {
// public:
//     Query query_from_json(TableRef table, nlohmann::json json) {
//         std::unique_ptr<AndNode> root = std:make_unique<AndNode>();
//         if (json["WhereClause"].is_object() {
//             root->atom_preds.emplace_back(parse_predicate(json["WhereClause"]));
//             //populate_and(json["WhereClause"], root->atom_preds);
//         }

//         return {};
//     }

//     void populate_and(json fragment, std::vector<std::unique_ptr<AtomPredNode>>& preds) {
//         if (fragment["kind"] == "and") {
//             populate_and(fragment["left"], preds);
//             populate_and(fragment["right"]);
//         }

//         preds.emplace_back(parse_predicate(fragment));
//     }

//     std::unique_ptr<AtomPredNode> parse_predicate(json fragment) {
//         switch (fragment["kind"]) {
//             case "and":
//                 auto parens = std::make_unique<ParensNode>();
//                 parens->pred = std::make_unique<OrNode>();
//                 auto and_node = std::make_unique<AndNode>();
//                 and_node->atom_preds.emplace_back(parse_predicate(fragment["left"]));
//                 and_node->atom_preds.emplace_back(parse_predicate(fragment["right"]));
//                 parens->pred->and_preds.emplace_back(std::move(and_node));
//                 return std::move(parens);
//             case "or":
//                 auto parens = std::make_unique<ParensNode>();
//                 parens->pred = std::make_unique<OrNode>();
//                 return std::move(parens);
//             case "eq":
//                 return nullptr;
//         }
//         REALM_UNREACHABLE();
//     }
// };

// std::unique_ptr<ValueNode> JsonQueryParser::get_subexpr_node(json json){
//     if (json["kind"] == "property"){
//         auto empty_path = std::make_unique<PathNode>();
//         auto prop_node = std::make_unique<PropNode>(std::move(empty_path), json["name"]);
//         auto value_node = std::make_unique<ValueNode>(std::move(prop_node));
//         return value_node;
//     } else if (json["kind"] == "constant"){
//         std::unique_ptr<ConstantNode> const_node;
//         if (json["type"] == "int"){
//             Int test = json["value"].get<Int>();
//             const_node = constant_node(test);
//         }
//         auto value_node = std::make_unique<ValueNode>(std::move(const_node));
//         return value_node;
//     }
// }


// {
//     "WhereClause": {
//         "kind": "not",
//         "expr": {
//             "kind":"or",
//             "left":{
//                 "kind":"eq",
//                 "left":{
//                     "kind":"property",
//                     "value":"Name",
//                     "type":"string"
//                 },
//                 "right":{
//                     "kind":"constant",
//                     "value":"John",
//                     "type":"string"
//                 }
//             },
//             "right":{
//                 "kind":"gt",
//                 "left":{
//                     "kind":"property",
//                     "value":"Age",
//                     "type":"float"
//                 },
//                 "right":{
//                     "kind":"constant",
//                     "value":1.0,
//                     "type":"float"
//                 }
//             }
//         }
//     }
// }


// {
//   "WhereClause": {
//     "kind": "Or",
//     "preds": [
//         {
//             "kind": "Eq",
//             "Left": {
//                 "Kind": "property",
//                 "Value": "Score",
//                 "Type": "float"
//             },
//             "Right": {
//                 "Kind": "constant",
//                 "Value": 5.0,
//                 "Type": "float"
//             }
//         },
//         {
//             "kind": "And",
//             "preds": [
//                 {
//                     "kind": "Gt",
//                     "Left": {
//                         "Kind": "property",
//                         "Value": "Score",
//                         "Type": "float"
//                     },
//                     "Right": {
//                         "Kind": "constant",
//                         "Value": 5.0,
//                         "Type": "float"
//                     }
//                 },
//                 {
//                     "kind": "paranthesis",
//                     "Left": {
//                         "Kind": "property",
//                         "Value": "Score",
//                         "Type": "float"
//                     },
//                     "Right": {
//                         "Kind": "constant",
//                         "Value": 5.0,
//                         "Type": "float"
//                     }
//                 },
//                 {
//                     "kind": "Eq",
//                     "Left": {
//                         "Kind": "property",
//                         "Value": "Score",
//                         "Type": "float"
//                     },
//                     "Right": {
//                         "Kind": "constant",
//                         "Value": 5.0,
//                         "Type": "float"
//                     }
//                 },
//             ]
//         },
//     ]
//   }
// }


{
   "whereClause":{
        "kind":"not",
        "expr":{
            "kind":"and",
            "left":{
                "kind":"eq",
                "left":{
                    "kind":"property",
                    "value":"Name",
                    "type":"string"
                },
                "right":{
                    "kind":"constant",
                    "value":"John",
                    "type":"string"
                }
            },
            "right":{
                "kind":"eq",
                "left":{
                    "kind":"property",
                    "value":"Name",
                    "type":"string"
                },
                "right":{
                    "kind":"constant",
                    "value":"John",
                    "type":"string"
                }
            }
        }
    }
}



{
   "whereClause":{
        "kind":"not",
        "expr":{
            "kind":"or",
            "left":{
                "kind":"eq",
                "left":{
                    "kind":"property",
                    "value":"Name",
                    "type":"string"
                },
                "right":{
                    "kind":"constant",
                    "value":"John",
                    "type":"string"
                }
            },
            "right":{
                "kind":"and",
                "left":{
                    "kind":"eq",
                    "left":{
                        "kind":"property",
                        "value":"Name",
                        "type":"string"
                    },
                    "right":{
                        "kind":"constant",
                        "value":"Doe",
                        "type":"string"
                    }
                },
                "right":{
                    "kind":"gt",
                    "left":{
                        "kind":"property",
                        "value":"Age",
                        "type":"float"
                    },
                    "right":{
                        "kind":"constant",
                        "value":1.0,
                        "type":"float"
                    }
                }
            }
        }
    }
}



{
   "whereClause":{
        "kind":"not",
        "expr":{
            "kind":"or",
            "left":{
                "kind":"eq",
                "left":{
                    "kind":"property",
                    "value":"Name",
                    "type":"string"
                },
                "right":{
                    "kind":"constant",
                    "value":"John",
                    "type":"string"
                }
            },
            "right":{
                "kind":"eq",
                "left":{
                    "kind":"property",
                    "value":"Name",
                    "type":"string"
                },
                "right":{
                    "kind":"constant",
                    "value":"John",
                    "type":"string"
                }
            }
        }
    }
}




{
  "whereClauses": [
    {
      "expression": {
        "kind": "or",
        "left": {
          "kind": "gt",
          "left": {
            "kind": "property",
            "value": "Age",
            "type": "int"
          },
          "right": {
            "kind": "constant",
            "value": 0,
            "type": "int"
          }
        },
        "right": {
          "kind": "and",
          "left": {
            "kind": "eq",
            "left": {
              "kind": "property",
              "value": "Age",
              "type": "int"
            },
            "right": {
              "kind": "constant",
              "value": 2,
              "type": "int"
            }
          },
          "right": {
            "kind": "eq",
            "left": {
              "kind": "property",
              "value": "Name",
              "type": "string"
            },
            "right": {
              "kind": "constant",
              "value": "John",
              "type": "string"
            }
          }
        }
      }
    }
  ],
  "orderingClauses": [
    {
      "isAscending": true,
      "property": "Age"
    },
    {
      "isAscending": true,
      "property": "Age2"
    }
  ]
}