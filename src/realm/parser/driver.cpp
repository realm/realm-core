#include "realm/parser/driver.hpp"
#include "realm/parser/keypath_mapping.hpp"
#include "realm/parser/query_parser.hpp"
#include "realm/sort_descriptor.hpp"
#include <realm/decimal128.hpp>
#include <realm/uuid.hpp>
#include "realm/util/base64.hpp"
#define YY_NO_UNISTD_H 1
#define YY_NO_INPUT 1
#include "realm/parser/generated/query_flex.hpp"

using namespace realm;
using namespace std::string_literals;

// Whether to generate parser debug traces.
static bool trace_parsing = false;
// Whether to generate scanner debug traces.
static bool trace_scanning = false;

namespace {

const char* agg_op_type_to_str(query_parser::AggrNode::Type type)
{
    switch (type) {
        case realm::query_parser::AggrNode::MAX:
            return ".@max";
        case realm::query_parser::AggrNode::MIN:
            return ".@min";
        case realm::query_parser::AggrNode::SUM:
            return ".@sum";
        case realm::query_parser::AggrNode::AVG:
            return ".@avg";
    }
    return "";
}

const char* expression_cmp_type_to_str(ExpressionComparisonType type)
{
    switch (type) {
        case ExpressionComparisonType::Any:
            return "ANY";
        case ExpressionComparisonType::All:
            return "ALL";
        case ExpressionComparisonType::None:
            return "NONE";
    }
    return "";
}

static std::map<int, std::string> opstr = {
    {CompareNode::EQUAL, "="},
    {CompareNode::NOT_EQUAL, "!="},
    {CompareNode::GREATER, ">"},
    {CompareNode::LESS, "<"},
    {CompareNode::GREATER_EQUAL, ">="},
    {CompareNode::LESS_EQUAL, "<="},
    {CompareNode::BEGINSWITH, "beginswith"},
    {CompareNode::ENDSWITH, "endswith"},
    {CompareNode::CONTAINS, "contains"},
    {CompareNode::LIKE, "like"},
    {CompareNode::IN, "in"},
};

std::string print_pretty_objlink(const ObjLink& link, const Group* g, ParserDriver* drv)
{
    REALM_ASSERT(g);
    if (link.is_null()) {
        return "NULL";
    }
    try {
        auto table = g->get_table(link.get_table_key());
        if (!table) {
            return "link to an invalid table";
        }
        auto obj = table->get_object(link.get_obj_key());
        Mixed pk = obj.get_primary_key();
        return util::format("'%1' with primary key '%2'", drv->get_printable_name(table->get_name()),
                            util::serializer::print_value(pk));
    }
    catch (...) {
        return "invalid link";
    }
}

bool is_length_suffix(const std::string& s)
{
    return s.size() == 6 && (s[0] == 'l' || s[0] == 'L') && (s[1] == 'e' || s[1] == 'E') &&
           (s[2] == 'n' || s[2] == 'N') && (s[3] == 'g' || s[3] == 'G') && (s[4] == 't' || s[4] == 'T') &&
           (s[5] == 'h' || s[5] == 'H');
}

template <typename T>
inline bool try_parse_specials(std::string str, T& ret)
{
    if constexpr (realm::is_any<T, float, double>::value || std::numeric_limits<T>::is_iec559) {
        std::transform(str.begin(), str.end(), str.begin(), toLowerAscii);
        if (std::numeric_limits<T>::has_quiet_NaN && (str == "nan" || str == "+nan")) {
            ret = std::numeric_limits<T>::quiet_NaN();
            return true;
        }
        else if (std::numeric_limits<T>::has_quiet_NaN && (str == "-nan")) {
            ret = -std::numeric_limits<T>::quiet_NaN();
            return true;
        }
        else if (std::numeric_limits<T>::has_infinity &&
                 (str == "+infinity" || str == "infinity" || str == "+inf" || str == "inf")) {
            ret = std::numeric_limits<T>::infinity();
            return true;
        }
        else if (std::numeric_limits<T>::has_infinity && (str == "-infinity" || str == "-inf")) {
            ret = -std::numeric_limits<T>::infinity();
            return true;
        }
    }
    return false;
}

template <typename T>
inline const char* get_type_name()
{
    return "unknown";
}
template <>
inline const char* get_type_name<int64_t>()
{
    return "number";
}
template <>
inline const char* get_type_name<float>()
{
    return "floating point number";
}
template <>
inline const char* get_type_name<double>()
{
    return "floating point number";
}

template <typename T>
inline T string_to(const std::string& s)
{
    std::istringstream iss(s);
    iss.imbue(std::locale::classic());
    T value;
    iss >> value;
    if (iss.fail()) {
        if (!try_parse_specials(s, value)) {
            throw InvalidQueryArgError(util::format("Cannot convert '%1' to a %2", s, get_type_name<T>()));
        }
    }
    return value;
}

class MixedArguments : public query_parser::Arguments {
public:
    MixedArguments(const std::vector<Mixed>& args)
        : Arguments(args.size())
        , m_args(args)
    {
    }
    bool bool_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<bool>();
    }
    long long long_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<int64_t>();
    }
    float float_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<float>();
    }
    double double_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<double>();
    }
    StringData string_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<StringData>();
    }
    BinaryData binary_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<BinaryData>();
    }
    Timestamp timestamp_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<Timestamp>();
    }
    ObjectId objectid_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<ObjectId>();
    }
    UUID uuid_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<UUID>();
    }
    Decimal128 decimal128_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<Decimal128>();
    }
    ObjKey object_index_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<ObjKey>();
    }
    ObjLink objlink_for_argument(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get<ObjLink>();
    }
    bool is_argument_null(size_t n) final
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).is_null();
    }
    DataType type_for_argument(size_t n)
    {
        Arguments::verify_ndx(n);
        return m_args.at(n).get_type();
    }

private:
    const std::vector<Mixed>& m_args;
};

} // namespace

namespace realm {

namespace query_parser {

NoArguments ParserDriver::s_default_args;
query_parser::KeyPathMapping ParserDriver::s_default_mapping;
using util::serializer::get_printable_table_name;

Arguments::~Arguments() {}

Timestamp get_timestamp_if_valid(int64_t seconds, int32_t nanoseconds)
{
    const bool both_non_negative = seconds >= 0 && nanoseconds >= 0;
    const bool both_non_positive = seconds <= 0 && nanoseconds <= 0;
    if (both_non_negative || both_non_positive) {
        return Timestamp(seconds, nanoseconds);
    }
    throw SyntaxError("Invalid timestamp format");
}

ParserNode::~ParserNode() {}

AtomPredNode::~AtomPredNode() {}

Query NotNode::visit(ParserDriver* drv)
{
    Query query = atom_preds[0]->visit(drv);
    Query q = drv->m_base_table->where();
    q.Not();
    q.and_query(query);
    return {q};
}

Query ParensNode::visit(ParserDriver* drv)
{
    return pred->visit(drv);
}

Query OrNode::visit(ParserDriver* drv)
{
    if (atom_preds.size() == 1) {
        return atom_preds[0]->visit(drv);
    }

    Query q(drv->m_base_table);
    q.group();
    for (auto& it : atom_preds) {
        q.Or();
        q.and_query(it->visit(drv));
    }
    q.end_group();

    return q;
}

Query AndNode::visit(ParserDriver* drv)
{
    if (atom_preds.size() == 1) {
        return atom_preds[0]->visit(drv);
    }
    Query q(drv->m_base_table);
    for (auto& it : atom_preds) {
        q.and_query(it->visit(drv));
    }
    return q;
}

void AndNode::accept(NodeVisitor& visitor)
{
    visitor.visitAnd(*this);
}

void NodeVisitor::visitAnd(AndNode& node)
{
    for (auto& pred : node.atom_preds) {
        pred->accept(*this);
    }
}

void OrNode::accept(NodeVisitor& visitor)
{
    visitor.visitOr(*this);
}

void NodeVisitor::visitOr(OrNode& node)
{
    for (auto& pred : node.atom_preds) {
        pred->accept(*this);
    }
}

void NotNode::accept(NodeVisitor& visitor)
{
    visitor.visitNot(*this);
}

void NodeVisitor::visitNot(NotNode& node)
{
    node.atom_preds[0]->accept(*this);
}

void TrueOrFalseNode::accept(NodeVisitor& visitor)
{
    visitor.visitTrueOrFalse(*this);
}

void NodeVisitor::visitTrueOrFalse(TrueOrFalseNode&)
{
    // leaf node, no further traversal
}

void ParensNode::accept(NodeVisitor& visitor)
{
    visitor.visitParens(*this);
}

void NodeVisitor::visitParens(ParensNode& node)
{
    this->visitOr(*node.pred);
}

void ConstantNode::accept(NodeVisitor& visitor)
{
    visitor.visitConstant(*this);
}

void NodeVisitor::visitConstant(ConstantNode&)
{
    // leaf node, no further traversal
}

void ValueNode::accept(NodeVisitor& visitor)
{
    visitor.visitValue(*this);
}

void NodeVisitor::visitValue(ValueNode& node)
{
    if (node.constant != nullptr) {
        this->visitConstant(*node.constant);
    }
    else {
        node.prop->accept(*this);
    }
}

void EqualityNode::accept(NodeVisitor& visitor)
{
    visitor.visitEquality(*this);
}

void NodeVisitor::visitEquality(EqualityNode& node)
{
    for (auto& value : node.values) {
        value->accept(*this);
    }
}

void RelationalNode::accept(NodeVisitor& visitor)
{
    visitor.visitRelational(*this);
}

void NodeVisitor::visitRelational(RelationalNode& node)
{
    for (auto& value : node.values) {
        value->accept(*this);
    }
}

void StringOpsNode::accept(NodeVisitor& visitor)
{
    visitor.visitStringOps(*this);
}

void NodeVisitor::visitStringOps(StringOpsNode& node)
{
    for (auto& value : node.values) {
        value->accept(*this);
    }
}

void PostOpNode::accept(NodeVisitor& visitor)
{
    visitor.visitPostOp(*this);
}

void NodeVisitor::visitPostOp(PostOpNode&)
{
    // leaf node, no further traversal
}

void AggrNode::accept(NodeVisitor& visitor)
{
    visitor.visitAggr(*this);
}

void NodeVisitor::visitAggr(AggrNode&)
{
    // leaf node, no further traversal
}

void PathNode::accept(NodeVisitor& visitor)
{
    visitor.visitPath(*this);
}

void NodeVisitor::visitPath(PathNode&)
{
    // leaf node, no further traversal
}

void ListAggrNode::accept(NodeVisitor& visitor)
{
    visitor.visitListAggr(*this);
}

void NodeVisitor::visitListAggr(ListAggrNode& node)
{
    this->visitPath(*node.path);
    this->visitAggr(*node.aggr_op);
}

void LinkAggrNode::accept(NodeVisitor& visitor)
{
    visitor.visitLinkAggr(*this);
}

void NodeVisitor::visitLinkAggr(LinkAggrNode& node)
{
    this->visitPath(*node.path);
    this->visitAggr(*node.aggr_op);
}

void PropNode::accept(NodeVisitor& visitor)
{
    visitor.visitProp(*this);
}

void NodeVisitor::visitProp(PropNode& node)
{
    this->visitPath(*node.path);
    if (node.post_op != nullptr) {
        this->visitPostOp(*node.post_op);
    }
    if (node.index != nullptr) {
        this->visitConstant(*node.index);
    }
}

void SubqueryNode::accept(NodeVisitor& visitor)
{
    visitor.visitSubquery(*this);
}

void NodeVisitor::visitSubquery(SubqueryNode& node)
{
    this->visitProp(*node.prop);
    this->visitOr(*node.subquery);
}

void DescriptorNode::accept(NodeVisitor& visitor)
{
    visitor.visitDescriptor(*this);
}

void NodeVisitor::visitDescriptor(DescriptorNode&)
{
    // leaf node, no further traversal
}

void DescriptorOrderingNode::accept(NodeVisitor& visitor)
{
    visitor.visitDescriptorOrdering(*this);
}

void NodeVisitor::visitDescriptorOrdering(DescriptorOrderingNode& node)
{
    for (auto& ordering : node.orderings) {
        this->visitDescriptor(*ordering);
    }
}

void verify_only_string_types(DataType type, const std::string& op_string)
{
    if (type != type_String && type != type_Binary && type != type_Mixed) {
        throw InvalidQueryError(util::format(
            "Unsupported comparison operator '%1' against type '%2', right side must be a string or binary type",
            op_string, get_data_type_name(type)));
    }
}

Query EqualityNode::visit(ParserDriver* drv)
{
    auto [left, right] = drv->cmp(std::move(values));

    auto left_type = left->get_type();
    auto right_type = right->get_type();

    if (left_type == type_Link && right_type == type_TypedLink && right->has_constant_evaluation()) {
        if (auto link_column = dynamic_cast<const Columns<Link>*>(left.get())) {
            if (right->get_mixed().is_null()) {
                right_type = ColumnTypeTraits<realm::null>::id;
                right = std::make_unique<Value<realm::null>>();
            }
            else {
                auto left_dest_table_key = link_column->link_map().get_target_table()->get_key();
                auto right_table_key = right->get_mixed().get_link().get_table_key();
                auto right_obj_key = right->get_mixed().get_link().get_obj_key();
                if (left_dest_table_key == right_table_key) {
                    right = std::make_unique<Value<ObjKey>>(right_obj_key);
                    right_type = type_Link;
                }
                else {
                    const Group* g = drv->m_base_table->get_parent_group();
                    throw std::invalid_argument(util::format(
                        "The relationship '%1' which links to type '%2' cannot be compared to an argument of type %3",
                        link_column->link_map().description(drv->m_serializer_state),
                        drv->get_printable_name(link_column->link_map().get_target_table()->get_name()),
                        print_pretty_objlink(right->get_mixed().get_link(), g, drv)));
                }
            }
        }
    }

    if (left_type.is_valid() && right_type.is_valid() && !Mixed::data_types_are_comparable(left_type, right_type)) {
        throw InvalidQueryError(util::format("Unsupported comparison between type '%1' and type '%2'",
                                             get_data_type_name(left_type), get_data_type_name(right_type)));
    }
    if (left_type == type_TypeOfValue || right_type == type_TypeOfValue) {
        if (left_type != right_type) {
            throw InvalidQueryArgError(
                util::format("Unsupported comparison between @type and raw value: '%1' and '%2'",
                             get_data_type_name(left_type), get_data_type_name(right_type)));
        }
    }

    if (op == CompareNode::IN) {
        Subexpr* r = right.get();
        if (!r->has_multiple_values()) {
            throw InvalidQueryArgError("The keypath following 'IN' must contain a list");
        }
    }

    const ObjPropertyBase* prop = dynamic_cast<const ObjPropertyBase*>(left.get());
    if (right->has_constant_evaluation() && (left_type == right_type || left_type == type_Mixed)) {
        Mixed val = right->get_mixed();
        if (prop && !prop->links_exist()) {
            auto col_key = prop->column_key();
            if (val.is_null()) {
                switch (op) {
                    case CompareNode::EQUAL:
                    case CompareNode::IN:
                        return drv->m_base_table->where().equal(col_key, realm::null());
                    case CompareNode::NOT_EQUAL:
                        return drv->m_base_table->where().not_equal(col_key, realm::null());
                }
            }
            switch (left->get_type()) {
                case type_Int:
                    return drv->simple_query(op, col_key, val.get_int());
                case type_Bool:
                    return drv->simple_query(op, col_key, val.get_bool());
                case type_String:
                    return drv->simple_query(op, col_key, val.get_string(), case_sensitive);
                case type_Binary:
                    return drv->simple_query(op, col_key, val.get_binary(), case_sensitive);
                case type_Timestamp:
                    return drv->simple_query(op, col_key, val.get<Timestamp>());
                case type_Float:
                    return drv->simple_query(op, col_key, val.get_float());
                case type_Double:
                    return drv->simple_query(op, col_key, val.get_double());
                case type_Decimal:
                    return drv->simple_query(op, col_key, val.get<Decimal128>());
                case type_ObjectId:
                    return drv->simple_query(op, col_key, val.get<ObjectId>());
                case type_UUID:
                    return drv->simple_query(op, col_key, val.get<UUID>());
                case type_Mixed:
                    return drv->simple_query(op, col_key, val, case_sensitive);
                default:
                    break;
            }
        }
        else if (left_type == type_Link) {
            auto link_column = dynamic_cast<const Columns<Link>*>(left.get());
            if (link_column && link_column->link_map().get_nb_hops() == 1 &&
                link_column->get_comparison_type() == ExpressionComparisonType::Any) {
                // We can use equal/not_equal and get a LinksToNode based query
                if (op == CompareNode::EQUAL) {
                    return drv->m_base_table->where().equal(link_column->link_map().get_first_column_key(), val);
                }
                else if (op == CompareNode::NOT_EQUAL) {
                    return drv->m_base_table->where().not_equal(link_column->link_map().get_first_column_key(), val);
                }
            }
        }
    }
    if (case_sensitive) {
        switch (op) {
            case CompareNode::EQUAL:
            case CompareNode::IN:
                return Query(std::unique_ptr<Expression>(new Compare<Equal>(std::move(right), std::move(left))));
            case CompareNode::NOT_EQUAL:
                return Query(std::unique_ptr<Expression>(new Compare<NotEqual>(std::move(right), std::move(left))));
        }
    }
    else {
        verify_only_string_types(right_type, opstr[op] + "[c]");
        switch (op) {
            case CompareNode::EQUAL:
            case CompareNode::IN:
                return Query(std::unique_ptr<Expression>(new Compare<EqualIns>(std::move(right), std::move(left))));
            case CompareNode::NOT_EQUAL:
                return Query(
                    std::unique_ptr<Expression>(new Compare<NotEqualIns>(std::move(right), std::move(left))));
        }
    }
    return {};
}

Query RelationalNode::visit(ParserDriver* drv)
{
    auto [left, right] = drv->cmp(std::move(values));

    auto left_type = left->get_type();
    auto right_type = right->get_type();
    const bool right_type_is_null = right->has_constant_evaluation() && right->get_mixed().is_null();
    const bool left_type_is_null = left->has_constant_evaluation() && left->get_mixed().is_null();
    REALM_ASSERT(!(left_type_is_null && right_type_is_null));

    if (left_type == type_Link || left_type == type_TypeOfValue) {
        throw InvalidQueryError(util::format(
            "Unsupported operator %1 in query. Only equal (==) and not equal (!=) are supported for this type.",
            opstr[op]));
    }

    if (!(left_type_is_null || right_type_is_null) && (!left_type.is_valid() || !right_type.is_valid() ||
                                                       !Mixed::data_types_are_comparable(left_type, right_type))) {
        throw InvalidQueryError(util::format("Unsupported comparison between type '%1' and type '%2'",
                                             get_data_type_name(left_type), get_data_type_name(right_type)));
    }

    const ObjPropertyBase* prop = dynamic_cast<const ObjPropertyBase*>(left.get());
    if (prop && !prop->links_exist() && right->has_constant_evaluation() &&
        (left_type == right_type || left_type == type_Mixed)) {
        auto col_key = prop->column_key();
        switch (left->get_type()) {
            case type_Int:
                return drv->simple_query(op, col_key, right->get_mixed().get_int());
            case type_Bool:
                break;
            case type_String:
                break;
            case type_Binary:
                break;
            case type_Timestamp:
                return drv->simple_query(op, col_key, right->get_mixed().get<Timestamp>());
            case type_Float:
                return drv->simple_query(op, col_key, right->get_mixed().get_float());
                break;
            case type_Double:
                return drv->simple_query(op, col_key, right->get_mixed().get_double());
                break;
            case type_Decimal:
                return drv->simple_query(op, col_key, right->get_mixed().get<Decimal128>());
                break;
            case type_ObjectId:
                return drv->simple_query(op, col_key, right->get_mixed().get<ObjectId>());
                break;
            case type_UUID:
                return drv->simple_query(op, col_key, right->get_mixed().get<UUID>());
                break;
            case type_Mixed:
                return drv->simple_query(op, col_key, right->get_mixed());
                break;
            default:
                break;
        }
    }
    switch (op) {
        case CompareNode::GREATER:
            return Query(std::unique_ptr<Expression>(new Compare<Less>(std::move(right), std::move(left))));
        case CompareNode::LESS:
            return Query(std::unique_ptr<Expression>(new Compare<Greater>(std::move(right), std::move(left))));
        case CompareNode::GREATER_EQUAL:
            return Query(std::unique_ptr<Expression>(new Compare<LessEqual>(std::move(right), std::move(left))));
        case CompareNode::LESS_EQUAL:
            return Query(std::unique_ptr<Expression>(new Compare<GreaterEqual>(std::move(right), std::move(left))));
    }
    return {};
}

Query StringOpsNode::visit(ParserDriver* drv)
{
    auto [left, right] = drv->cmp(std::move(values));

    auto left_type = left->get_type();
    auto right_type = right->get_type();
    const ObjPropertyBase* prop = dynamic_cast<const ObjPropertyBase*>(left.get());

    verify_only_string_types(right_type, opstr[op]);

    if (prop && !prop->links_exist() && right->has_constant_evaluation() &&
        (left_type == right_type || left_type == type_Mixed)) {
        auto col_key = prop->column_key();
        if (right_type == type_String) {
            StringData val = right->get_mixed().get_string();

            switch (op) {
                case CompareNode::BEGINSWITH:
                    return drv->m_base_table->where().begins_with(col_key, val, case_sensitive);
                case CompareNode::ENDSWITH:
                    return drv->m_base_table->where().ends_with(col_key, val, case_sensitive);
                case CompareNode::CONTAINS:
                    return drv->m_base_table->where().contains(col_key, val, case_sensitive);
                case CompareNode::LIKE:
                    return drv->m_base_table->where().like(col_key, val, case_sensitive);
            }
        }
        else if (right_type == type_Binary) {
            BinaryData val = right->get_mixed().get_binary();

            switch (op) {
                case CompareNode::BEGINSWITH:
                    return drv->m_base_table->where().begins_with(col_key, val, case_sensitive);
                case CompareNode::ENDSWITH:
                    return drv->m_base_table->where().ends_with(col_key, val, case_sensitive);
                case CompareNode::CONTAINS:
                    return drv->m_base_table->where().contains(col_key, val, case_sensitive);
                case CompareNode::LIKE:
                    return drv->m_base_table->where().like(col_key, val, case_sensitive);
            }
        }
    }

    if (case_sensitive) {
        switch (op) {
            case CompareNode::BEGINSWITH:
                return Query(std::unique_ptr<Expression>(new Compare<BeginsWith>(std::move(right), std::move(left))));
            case CompareNode::ENDSWITH:
                return Query(std::unique_ptr<Expression>(new Compare<EndsWith>(std::move(right), std::move(left))));
            case CompareNode::CONTAINS:
                return Query(std::unique_ptr<Expression>(new Compare<Contains>(std::move(right), std::move(left))));
            case CompareNode::LIKE:
                return Query(std::unique_ptr<Expression>(new Compare<Like>(std::move(right), std::move(left))));
        }
    }
    else {
        switch (op) {
            case CompareNode::BEGINSWITH:
                return Query(
                    std::unique_ptr<Expression>(new Compare<BeginsWithIns>(std::move(right), std::move(left))));
            case CompareNode::ENDSWITH:
                return Query(
                    std::unique_ptr<Expression>(new Compare<EndsWithIns>(std::move(right), std::move(left))));
            case CompareNode::CONTAINS:
                return Query(
                    std::unique_ptr<Expression>(new Compare<ContainsIns>(std::move(right), std::move(left))));
            case CompareNode::LIKE:
                return Query(std::unique_ptr<Expression>(new Compare<LikeIns>(std::move(right), std::move(left))));
        }
    }
    return {};
}

Query TrueOrFalseNode::visit(ParserDriver* drv)
{
    Query q = drv->m_base_table->where();
    if (true_or_false) {
        q.and_query(std::unique_ptr<realm::Expression>(new TrueExpression));
    }
    else {
        q.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
    }
    return q;
}

std::unique_ptr<Subexpr> PropNode::visit(ParserDriver* drv)
{
    bool is_keys = false;
    if (identifier[0] == '@') {
        if (identifier == "@values") {
            identifier = path->path_elems.back();
            path->path_elems.pop_back();
        }
        else if (identifier == "@keys") {
            identifier = path->path_elems.back();
            path->path_elems.pop_back();
            is_keys = true;
        }
        else if (identifier == "@links") {
            // This is a backlink aggregate query
            auto link_chain = path->visit(drv, comp_type);
            auto sub = link_chain.get_backlink_count<Int>();
            return sub.clone();
        }
    }
    try {
        auto link_chain = path->visit(drv, comp_type);
        std::unique_ptr<Subexpr> subexpr{drv->column(link_chain, identifier)};
        if (index) {
            if (auto s = dynamic_cast<Columns<Dictionary>*>(subexpr.get())) {
                auto t = s->get_type();
                auto idx = index->visit(drv, t);
                Mixed key = idx->get_mixed();
                subexpr = s->key(key).clone();
            }
        }
        if (is_keys) {
            if (auto s = dynamic_cast<Columns<Dictionary>*>(subexpr.get())) {
                subexpr = std::make_unique<ColumnDictionaryKeys>(*s);
            }
        }

        if (post_op) {
            return post_op->visit(drv, subexpr.get());
        }
        return subexpr;
    }
    catch (const std::runtime_error& e) {
        // Is 'identifier' perhaps length operator?
        if (!post_op && is_length_suffix(identifier) && path->path_elems.size() > 0) {
            // If 'length' is the operator, the last id in the path must be the name
            // of a list property
            auto prop = path->path_elems.back();
            path->path_elems.pop_back();
            std::unique_ptr<Subexpr> subexpr{path->visit(drv, comp_type).column(prop)};
            if (auto list = dynamic_cast<ColumnListBase*>(subexpr.get())) {
                if (auto length_expr = list->get_element_length())
                    return length_expr;
            }
        }
        throw InvalidQueryError(e.what());
    }
    REALM_UNREACHABLE();
    return {};
}

std::unique_ptr<Subexpr> SubqueryNode::visit(ParserDriver* drv)
{
    if (variable_name.size() < 2 || variable_name[0] != '$') {
        throw SyntaxError(util::format("The subquery variable '%1' is invalid. The variable must start with "
                                       "'$' and cannot be empty; for example '$x'.",
                                       variable_name));
    }
    LinkChain lc = prop->path->visit(drv, prop->comp_type);
    prop->identifier = drv->translate(lc, prop->identifier);

    if (prop->identifier.find("@links") == 0) {
        drv->backlink(lc, prop->identifier);
    }
    else {
        ColKey col_key = lc.get_current_table()->get_column_key(prop->identifier);
        if (col_key.is_list() && col_key.get_type() != col_type_LinkList) {
            throw InvalidQueryError(util::format(
                "A subquery can not operate on a list of primitive values (property '%1')", prop->identifier));
        }
        if (col_key.get_type() != col_type_LinkList) {
            throw InvalidQueryError(util::format("A subquery must operate on a list property, but '%1' is type '%2'",
                                                 prop->identifier,
                                                 realm::get_data_type_name(DataType(col_key.get_type()))));
        }
        lc.link(prop->identifier);
    }
    TableRef previous_table = drv->m_base_table;
    drv->m_base_table = lc.get_current_table().cast_away_const();
    bool did_add = drv->m_mapping.add_mapping(drv->m_base_table, variable_name, "");
    if (!did_add) {
        throw InvalidQueryError(util::format("Unable to create a subquery expression with variable '%1' since an "
                                             "identical variable already exists in this context",
                                             variable_name));
    }
    Query sub = subquery->visit(drv);
    drv->m_mapping.remove_mapping(drv->m_base_table, variable_name);
    drv->m_base_table = previous_table;

    return std::unique_ptr<Subexpr>(lc.subquery(sub));
}

std::unique_ptr<Subexpr> PostOpNode::visit(ParserDriver*, Subexpr* subexpr)
{
    if (op_type == PostOpNode::SIZE) {
        if (auto s = dynamic_cast<Columns<Link>*>(subexpr)) {
            return s->count().clone();
        }
        if (auto s = dynamic_cast<ColumnListBase*>(subexpr)) {
            return s->size().clone();
        }
        if (auto s = dynamic_cast<Columns<StringData>*>(subexpr)) {
            return s->size().clone();
        }
        if (auto s = dynamic_cast<Columns<BinaryData>*>(subexpr)) {
            return s->size().clone();
        }
    }
    else if (op_type == PostOpNode::TYPE) {
        if (auto s = dynamic_cast<Columns<Mixed>*>(subexpr)) {
            return s->type_of_value().clone();
        }
        if (auto s = dynamic_cast<ColumnsCollection<Mixed>*>(subexpr)) {
            return s->type_of_value().clone();
        }
        if (auto s = dynamic_cast<ObjPropertyBase*>(subexpr)) {
            return Value<TypeOfValue>(TypeOfValue(s->column_key())).clone();
        }
        if (dynamic_cast<Columns<Link>*>(subexpr)) {
            return Value<TypeOfValue>(TypeOfValue(TypeOfValue::Attribute::ObjectLink)).clone();
        }
    }

    if (subexpr) {
        throw InvalidQueryError(util::format("Operation '%1' is not supported on property of type '%2'", op_name,
                                             get_data_type_name(DataType(subexpr->get_type()))));
    }
    REALM_UNREACHABLE();
    return {};
}

std::unique_ptr<Subexpr> LinkAggrNode::visit(ParserDriver* drv)
{
    auto link_chain = path->visit(drv);
    auto subexpr = std::unique_ptr<Subexpr>(drv->column(link_chain, link));
    auto link_prop = dynamic_cast<Columns<Link>*>(subexpr.get());
    if (!link_prop) {
        throw InvalidQueryError(util::format("Operation '%1' cannot apply to property '%2' because it is not a list",
                                             agg_op_type_to_str(aggr_op->type), link));
    }
    prop = drv->translate(link_chain, prop);
    auto col_key = link_chain.get_current_table()->get_column_key(prop);

    std::unique_ptr<Subexpr> sub_column;
    switch (col_key.get_type()) {
        case col_type_Int:
            sub_column = link_prop->column<Int>(col_key).clone();
            break;
        case col_type_Float:
            sub_column = link_prop->column<float>(col_key).clone();
            break;
        case col_type_Double:
            sub_column = link_prop->column<double>(col_key).clone();
            break;
        case col_type_Decimal:
            sub_column = link_prop->column<Decimal>(col_key).clone();
            break;
        case col_type_Timestamp:
            sub_column = link_prop->column<Timestamp>(col_key).clone();
            break;
        default:
            throw InvalidQueryError(util::format("collection aggregate not supported for type '%1'",
                                                 get_data_type_name(DataType(col_key.get_type()))));
    }
    return aggr_op->visit(drv, sub_column.get());
}

std::unique_ptr<Subexpr> ListAggrNode::visit(ParserDriver* drv)
{
    auto link_chain = path->visit(drv);
    std::unique_ptr<Subexpr> subexpr{drv->column(link_chain, identifier)};
    return aggr_op->visit(drv, subexpr.get());
}

std::unique_ptr<Subexpr> AggrNode::visit(ParserDriver*, Subexpr* subexpr)
{
    std::unique_ptr<Subexpr> agg;
    if (auto list_prop = dynamic_cast<ColumnListBase*>(subexpr)) {
        switch (type) {
            case MAX:
                agg = list_prop->max_of();
                break;
            case MIN:
                agg = list_prop->min_of();
                break;
            case SUM:
                agg = list_prop->sum_of();
                break;
            case AVG:
                agg = list_prop->avg_of();
                break;
        }
    }
    else if (auto prop = dynamic_cast<SubColumnBase*>(subexpr)) {
        switch (type) {
            case MAX:
                agg = prop->max_of();
                break;
            case MIN:
                agg = prop->min_of();
                break;
            case SUM:
                agg = prop->sum_of();
                break;
            case AVG:
                agg = prop->avg_of();
                break;
        }
    }
    if (!agg) {
        throw InvalidQueryError(
            util::format("Cannot use aggregate '%1' for this type of property", agg_op_type_to_str(type)));
    }

    return agg;
}

std::unique_ptr<Subexpr> ConstantNode::visit(ParserDriver* drv, DataType hint)
{
    Subexpr* ret = nullptr;
    std::string explain_value_message = text;
    switch (type) {
        case Type::NUMBER: {
            if (hint == type_Decimal) {
                ret = new Value<Decimal128>(Decimal128(text));
            }
            else {
                ret = new Value<int64_t>(strtoll(text.c_str(), nullptr, 0));
            }
            break;
        }
        case Type::FLOAT: {
            switch (hint) {
                case type_Float: {
                    ret = new Value<float>(strtof(text.c_str(), nullptr));
                    break;
                }
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128(text));
                    break;
                default:
                    ret = new Value<double>(strtod(text.c_str(), nullptr));
                    break;
            }
            break;
        }
        case Type::INFINITY_VAL: {
            bool negative = text[0] == '-';
            switch (hint) {
                case type_Float: {
                    auto inf = std::numeric_limits<float>::infinity();
                    ret = new Value<float>(negative ? -inf : inf);
                    break;
                }
                case type_Double: {
                    auto inf = std::numeric_limits<double>::infinity();
                    ret = new Value<double>(negative ? -inf : inf);
                    break;
                }
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128(text));
                    break;
                default:
                    throw InvalidQueryError(util::format("Infinity not supported for %1", get_data_type_name(hint)));
                    break;
            }
            break;
        }
        case Type::NAN_VAL: {
            switch (hint) {
                case type_Float:
                    ret = new Value<float>(type_punning<float>(0x7fc00000));
                    break;
                case type_Double:
                    ret = new Value<double>(type_punning<double>(0x7ff8000000000000));
                    break;
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128::nan("0"));
                    break;
                default:
                    REALM_UNREACHABLE();
                    break;
            }
            break;
        }
        case Type::STRING: {
            std::string str = text.substr(1, text.size() - 2);
            switch (hint) {
                case type_Int:
                    ret = new Value<int64_t>(string_to<int64_t>(str));
                    break;
                case type_Float:
                    ret = new Value<float>(string_to<float>(str));
                    break;
                case type_Double:
                    ret = new Value<double>(string_to<double>(str));
                    break;
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128(str.c_str()));
                    break;
                default:
                    if (hint == type_TypeOfValue) {
                        try {
                            ret = new Value<TypeOfValue>(TypeOfValue(str));
                        }
                        catch (const std::runtime_error& e) {
                            throw InvalidQueryArgError(e.what());
                        }
                    }
                    else {
                        ret = new ConstantStringValue(str);
                    }
                    break;
            }
            break;
        }
        case Type::BASE64: {
            const size_t encoded_size = text.size() - 5;
            size_t buffer_size = util::base64_decoded_size(encoded_size);
            drv->m_args.buffer_space.push_back({});
            auto& decode_buffer = drv->m_args.buffer_space.back();
            decode_buffer.resize(buffer_size);
            StringData window(text.c_str() + 4, encoded_size);
            util::Optional<size_t> decoded_size = util::base64_decode(window, decode_buffer.data(), buffer_size);
            if (!decoded_size) {
                throw SyntaxError("Invalid base64 value");
            }
            REALM_ASSERT_DEBUG_EX(*decoded_size <= encoded_size, *decoded_size, encoded_size);
            decode_buffer.resize(*decoded_size); // truncate

            if (hint == type_String) {
                ret = new ConstantStringValue(StringData(decode_buffer.data(), decode_buffer.size()));
            }
            if (hint == type_Binary) {
                ret = new Value<BinaryData>(BinaryData(decode_buffer.data(), decode_buffer.size()));
            }
            if (hint == type_Mixed) {
                ret = new Value<BinaryData>(BinaryData(decode_buffer.data(), decode_buffer.size()));
            }
            break;
        }
        case Type::TIMESTAMP: {
            auto s = text;
            int64_t seconds;
            int32_t nanoseconds;
            if (s[0] == 'T') {
                size_t colon_pos = s.find(":");
                std::string s1 = s.substr(1, colon_pos - 1);
                std::string s2 = s.substr(colon_pos + 1);
                seconds = strtol(s1.c_str(), nullptr, 0);
                nanoseconds = int32_t(strtol(s2.c_str(), nullptr, 0));
            }
            else {
                // readable format YYYY-MM-DD-HH:MM:SS:NANOS nanos optional
                struct tm tmp = tm();
                char sep = s.find("@") < s.size() ? '@' : 'T';
                std::string fmt = "%d-%d-%d"s + sep + "%d:%d:%d:%d"s;
                int cnt = sscanf(s.c_str(), fmt.c_str(), &tmp.tm_year, &tmp.tm_mon, &tmp.tm_mday, &tmp.tm_hour,
                                 &tmp.tm_min, &tmp.tm_sec, &nanoseconds);
                REALM_ASSERT(cnt >= 6);
                tmp.tm_year -= 1900; // epoch offset (see man mktime)
                tmp.tm_mon -= 1;     // converts from 1-12 to 0-11

                if (tmp.tm_year < 0) {
                    // platform timegm functions do not throw errors, they return -1 which is also a valid time
                    throw InvalidQueryError("Conversion of dates before 1900 is not supported.");
                }

                seconds = platform_timegm(tmp); // UTC time
                if (cnt == 6) {
                    nanoseconds = 0;
                }
                if (nanoseconds < 0) {
                    throw SyntaxError("The nanoseconds of a Timestamp cannot be negative.");
                }
                if (seconds < 0) { // seconds determines the sign of the nanoseconds part
                    nanoseconds *= -1;
                }
            }
            ret = new Value<Timestamp>(get_timestamp_if_valid(seconds, nanoseconds));
            break;
        }
        case Type::UUID_T:
            ret = new Value<UUID>(UUID(text.substr(5, text.size() - 6)));
            break;
        case Type::OID:
            ret = new Value<ObjectId>(ObjectId(text.substr(4, text.size() - 5).c_str()));
            break;
        case Type::LINK: {
            ret = new Value<ObjKey>(ObjKey(strtol(text.substr(1, text.size() - 1).c_str(), nullptr, 0)));
            break;
        }
        case Type::TYPED_LINK: {
            size_t colon_pos = text.find(":");
            auto table_key_val = uint32_t(strtol(text.substr(1, colon_pos - 1).c_str(), nullptr, 0));
            auto obj_key_val = strtol(text.substr(colon_pos + 1).c_str(), nullptr, 0);
            ret = new Value<ObjLink>(ObjLink(TableKey(table_key_val), ObjKey(obj_key_val)));
            break;
        }
        case Type::NULL_VAL:
            if (hint == type_String) {
                ret = new ConstantStringValue(StringData()); // Null string
            }
            else if (hint == type_Binary) {
                ret = new Value<Binary>(BinaryData()); // Null string
            }
            else {
                ret = new Value<null>(realm::null());
            }
            break;
        case Type::TRUE:
            ret = new Value<Bool>(true);
            break;
        case Type::FALSE:
            ret = new Value<Bool>(false);
            break;
        case Type::ARG: {
            size_t arg_no = size_t(strtol(text.substr(1).c_str(), nullptr, 10));
            if (drv->m_args.is_argument_null(arg_no)) {
                explain_value_message = util::format("argument '%1' which is NULL", explain_value_message);
                ret = new Value<null>(realm::null());
            }
            else {
                auto type = drv->m_args.type_for_argument(arg_no);
                explain_value_message =
                    util::format("argument %1 of type '%2'", explain_value_message, get_data_type_name(type));
                switch (type) {
                    case type_Int:
                        ret = new Value<int64_t>(drv->m_args.long_for_argument(arg_no));
                        break;
                    case type_String:
                        ret = new ConstantStringValue(drv->m_args.string_for_argument(arg_no));
                        break;
                    case type_Binary:
                        ret = new ConstantBinaryValue(drv->m_args.binary_for_argument(arg_no));
                        break;
                    case type_Bool:
                        ret = new Value<Bool>(drv->m_args.bool_for_argument(arg_no));
                        break;
                    case type_Float:
                        ret = new Value<float>(drv->m_args.float_for_argument(arg_no));
                        break;
                    case type_Double: {
                        // In realm-js all number type arguments are returned as double. If we don't cast to the
                        // expected type, we would in many cases miss the option to use the optimized query node
                        // instead of the general Compare class.
                        double val = drv->m_args.double_for_argument(arg_no);
                        switch (hint) {
                            case type_Int:
                            case type_Bool: {
                                int64_t int_val = int64_t(val);
                                // Only return an integer if it precisely represents val
                                if (double(int_val) == val)
                                    ret = new Value<int64_t>(int_val);
                                else
                                    ret = new Value<double>(val);
                                break;
                            }
                            case type_Float:
                                ret = new Value<float>(float(val));
                                break;
                            default:
                                ret = new Value<double>(val);
                                break;
                        }
                        break;
                    }
                    case type_Timestamp: {
                        try {
                            ret = new Value<Timestamp>(drv->m_args.timestamp_for_argument(arg_no));
                        }
                        catch (const std::exception&) {
                            ret = new Value<ObjectId>(drv->m_args.objectid_for_argument(arg_no));
                        }
                        break;
                    }
                    case type_ObjectId: {
                        try {
                            ret = new Value<ObjectId>(drv->m_args.objectid_for_argument(arg_no));
                        }
                        catch (const std::exception&) {
                            ret = new Value<Timestamp>(drv->m_args.timestamp_for_argument(arg_no));
                        }
                        break;
                    }
                    case type_Decimal:
                        ret = new Value<Decimal128>(drv->m_args.decimal128_for_argument(arg_no));
                        break;
                    case type_UUID:
                        ret = new Value<UUID>(drv->m_args.uuid_for_argument(arg_no));
                        break;
                    case type_Link:
                        ret = new Value<ObjKey>(drv->m_args.object_index_for_argument(arg_no));
                        break;
                    case type_TypedLink:
                        if (hint == type_Mixed || hint == type_Link || hint == type_TypedLink) {
                            ret = new Value<ObjLink>(drv->m_args.objlink_for_argument(arg_no));
                            break;
                        }
                        explain_value_message =
                            util::format("%1 which links to %2", explain_value_message,
                                         print_pretty_objlink(drv->m_args.objlink_for_argument(arg_no),
                                                              drv->m_base_table->get_parent_group(), drv));
                        break;
                    default:
                        break;
                }
            }
            break;
        }
    }
    if (!ret) {
        throw InvalidQueryError(
            util::format("Unsupported comparison between property of type '%1' and constant value: %2",
                         get_data_type_name(hint), explain_value_message));
    }
    return std::unique_ptr<Subexpr>{ret};
}

LinkChain PathNode::visit(ParserDriver* drv, ExpressionComparisonType comp_type)
{
    LinkChain link_chain(drv->m_base_table, comp_type);
    for (std::string path_elem : path_elems) {
        path_elem = drv->translate(link_chain, path_elem);
        if (path_elem.find("@links.") == 0) {
            drv->backlink(link_chain, path_elem);
        }
        else if (path_elem == "@values") {
            if (!link_chain.get_current_col().is_dictionary()) {
                throw InvalidQueryError("@values only allowed on dictionaries");
            }
            continue;
        }
        else if (path_elem.empty()) {
            continue; // this element has been removed, this happens in subqueries
        }
        else {
            try {
                link_chain.link(path_elem);
            }
            // I case of exception, we have to throw InvalidQueryError
            catch (const std::runtime_error& e) {
                auto str = e.what();
                StringData table_name = drv->get_printable_name(link_chain.get_current_table()->get_name());
                if (strstr(str, "no property")) {
                    throw InvalidQueryError(util::format("'%1' has no property: '%2'", table_name, path_elem));
                }
                else {
                    throw InvalidQueryError(
                        util::format("Property '%1' in '%2' is not an Object", path_elem, table_name));
                }
            }
        }
    }
    return link_chain;
}

DescriptorNode::~DescriptorNode() {}

DescriptorOrderingNode::~DescriptorOrderingNode() {}

std::unique_ptr<DescriptorOrdering> DescriptorOrderingNode::visit(ParserDriver* drv)
{
    auto target = drv->m_base_table;
    std::unique_ptr<DescriptorOrdering> ordering;
    for (auto& cur_ordering : orderings) {
        if (!ordering)
            ordering = std::make_unique<DescriptorOrdering>();
        if (cur_ordering->get_type() == DescriptorNode::LIMIT) {
            ordering->append_limit(LimitDescriptor(cur_ordering->limit));
        }
        else {
            bool is_distinct = cur_ordering->get_type() == DescriptorNode::DISTINCT;
            std::vector<std::vector<ColKey>> property_columns;
            for (auto& col_names : cur_ordering->columns) {
                std::vector<ColKey> columns;
                LinkChain link_chain(target);
                for (size_t ndx_in_path = 0; ndx_in_path < col_names.size(); ++ndx_in_path) {
                    std::string path_elem = drv->translate(link_chain, col_names[ndx_in_path]);
                    ColKey col_key = link_chain.get_current_table()->get_column_key(path_elem);
                    if (!col_key) {
                        throw InvalidQueryError(
                            util::format("No property '%1' found on object type '%2' specified in '%3' clause",
                                         col_names[ndx_in_path],
                                         drv->get_printable_name(link_chain.get_current_table()->get_name()),
                                         is_distinct ? "distinct" : "sort"));
                    }
                    columns.push_back(col_key);
                    if (ndx_in_path < col_names.size() - 1) {
                        link_chain.link(col_key);
                    }
                }
                property_columns.push_back(columns);
            }

            if (is_distinct) {
                ordering->append_distinct(DistinctDescriptor(property_columns));
            }
            else {
                ordering->append_sort(SortDescriptor(property_columns, cur_ordering->ascending),
                                      cur_ordering->merge_mode);
            }
        }
    }

    return ordering;
}

// If one of the expresions is constant, it should be right
void verify_conditions(Subexpr* left, Subexpr* right, util::serializer::SerialisationState& state)
{
    if (dynamic_cast<ColumnListBase*>(left) && dynamic_cast<ColumnListBase*>(right)) {
        throw InvalidQueryError(
            util::format("Ordered comparison between two primitive lists is not implemented yet ('%1' and '%2')",
                         left->description(state), right->description(state)));
    }
    if (left->has_multiple_values() && right->has_multiple_values()) {
        throw InvalidQueryError(util::format("Comparison between two lists is not supported ('%1' and '%2')",
                                             left->description(state), right->description(state)));
    }
    if (dynamic_cast<Value<TypeOfValue>*>(left) && dynamic_cast<Value<TypeOfValue>*>(right)) {
        throw InvalidQueryError(util::format("Comparison between two constants is not supported ('%1' and '%2')",
                                             left->description(state), right->description(state)));
    }
    if (auto link_column = dynamic_cast<Columns<Link>*>(left)) {
        if (link_column->has_multiple_values() && right->has_constant_evaluation() && right->get_mixed().is_null()) {
            throw InvalidQueryError(
                util::format("Cannot compare linklist ('%1') with NULL", left->description(state)));
        }
    }
}

ParserDriver::ParserDriver(TableRef t, Arguments& args, const query_parser::KeyPathMapping& mapping)
    : m_serializer_state(mapping.get_backlink_class_prefix())
    , m_base_table(t)
    , m_args(args)
    , m_mapping(mapping)
{
    yylex_init(&m_yyscanner);
}

ParserDriver::~ParserDriver()
{
    yylex_destroy(m_yyscanner);
}


std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>>
ParserDriver::cmp(std::vector<std::unique_ptr<ValueNode>>&& values)
{
    std::unique_ptr<Subexpr> left;
    std::unique_ptr<Subexpr> right;

    auto left_constant = std::move(values[0]->constant);
    auto right_constant = std::move(values[1]->constant);
    auto left_prop = std::move(values[0]->prop);
    auto right_prop = std::move(values[1]->prop);

    if (left_constant && right_constant) {
        throw InvalidQueryError("Cannot compare two constants");
    }

    if (right_constant) {
        // Take left first - it cannot be a constant
        left = left_prop->visit(this);
        right = right_constant->visit(this, left->get_type());
        verify_conditions(left.get(), right.get(), m_serializer_state);
    }
    else {
        right = right_prop->visit(this);
        if (left_constant) {
            left = left_constant->visit(this, right->get_type());
        }
        else {
            left = left_prop->visit(this);
        }
        verify_conditions(right.get(), left.get(), m_serializer_state);
    }
    return {std::move(left), std::move(right)};
}

Subexpr* ParserDriver::column(LinkChain& link_chain, std::string identifier)
{
    identifier = m_mapping.translate(link_chain, identifier);

    if (identifier.find("@links.") == 0) {
        backlink(link_chain, identifier);
        return link_chain.create_subexpr<Link>(ColKey());
    }
    if (auto col = link_chain.column(identifier)) {
        return col;
    }
    throw InvalidQueryError(util::format("'%1' has no property: '%2'",
                                         get_printable_name(link_chain.get_current_table()->get_name()), identifier));
}

void ParserDriver::backlink(LinkChain& link_chain, const std::string& identifier)
{
    auto table_column_pair = identifier.substr(7);
    auto dot_pos = table_column_pair.find('.');

    auto table_name = table_column_pair.substr(0, dot_pos);
    table_name = m_mapping.translate_table_name(table_name);
    auto origin_table = m_base_table->get_parent_group()->get_table(table_name);
    auto column_name = table_column_pair.substr(dot_pos + 1);
    ColKey origin_column;
    if (origin_table) {
        column_name = m_mapping.translate(origin_table, column_name);
        origin_column = origin_table->get_column_key(column_name);
    }
    if (!origin_column) {
        auto current_table_name = link_chain.get_current_table()->get_name();
        throw InvalidQueryError(util::format("No property '%1' found in type '%2' which links to type '%3'",
                                             column_name, get_printable_name(table_name),
                                             get_printable_name(current_table_name)));
    }
    link_chain.backlink(*origin_table, origin_column);
}

std::string ParserDriver::translate(LinkChain& link_chain, const std::string& identifier)
{
    return m_mapping.translate(link_chain, identifier);
}

StringData ParserDriver::get_printable_name(StringData table_name) const
{
    return util::serializer::get_printable_table_name(table_name, m_serializer_state.class_prefix);
}

int ParserDriver::parse(const std::string& str)
{
    // std::cout << str << std::endl;
    parse_buffer.append(str);
    parse_buffer.append("\0\0", 2); // Flex requires 2 terminating zeroes
    scan_begin(m_yyscanner, trace_scanning);
    yy::parser parse(*this, m_yyscanner);
    parse.set_debug_level(trace_parsing);
    int res = parse();
    if (parse_error) {
        std::string msg = "Invalid predicate: '" + str + "': " + error_string;
        throw SyntaxError(msg);
    }
    return res;
}

void parse(const std::string& str)
{
    ParserDriver driver;
    driver.parse(str);
}

std::string check_escapes(const char* str)
{
    std::string ret;
    const char* p = strchr(str, '\\');
    while (p) {
        ret += std::string(str, p);
        p++;
        if (*p == ' ') {
            ret += ' ';
        }
        else if (*p == 't') {
            ret += '\t';
        }
        else if (*p == 'r') {
            ret += '\r';
        }
        else if (*p == 'n') {
            ret += '\n';
        }
        str = p + 1;
        p = strchr(str, '\\');
    }
    return ret + std::string(str);
}

} // namespace query_parser

Query Table::query(const std::string& query_string, const std::vector<Mixed>& arguments) const
{
    MixedArguments args(arguments);
    return query(query_string, args, {});
}

Query Table::query(const std::string& query_string, const std::vector<Mixed>& arguments,
                   const query_parser::KeyPathMapping& mapping) const
{
    MixedArguments args(arguments);
    return query(query_string, args, mapping);
}

Query Table::query(const std::string& query_string, query_parser::Arguments& args,
                   const query_parser::KeyPathMapping& mapping) const
{
    ParserDriver driver(m_own_ref, args, mapping);
    driver.parse(query_string);

    // Query query = QueryVisitor(&driver).visit(*driver.result);
    // return query.set_ordering(QueryVisitor(&driver).getDescriptorOrdering(driver.ordering));

    return driver.result->visit(&driver).set_ordering(driver.ordering->visit(&driver));
}

Query Table::query_new(const std::string& query_string) const
{
    auto jsonObj = nlohmann::json::parse(query_string);
    return JsonQueryParser().query_from_json(m_own_ref, jsonObj);
}

Subexpr* LinkChain::column(const std::string& col)
{
    auto col_key = m_current_table->get_column_key(col);
    if (!col_key) {
        return nullptr;
    }
    size_t list_count = 0;
    for (ColKey link_key : m_link_cols) {
        if (link_key.get_type() == col_type_LinkList || link_key.get_type() == col_type_BackLink) {
            list_count++;
        }
    }

    if (col_key.is_dictionary()) {
        return create_subexpr<Dictionary>(col_key);
    }
    else if (col_key.is_set()) {
        switch (col_key.get_type()) {
            case col_type_Int:
                return create_subexpr<Set<Int>>(col_key);
            case col_type_Bool:
                return create_subexpr<Set<Bool>>(col_key);
            case col_type_String:
                return create_subexpr<Set<String>>(col_key);
            case col_type_Binary:
                return create_subexpr<Set<Binary>>(col_key);
            case col_type_Float:
                return create_subexpr<Set<Float>>(col_key);
            case col_type_Double:
                return create_subexpr<Set<Double>>(col_key);
            case col_type_Timestamp:
                return create_subexpr<Set<Timestamp>>(col_key);
            case col_type_Decimal:
                return create_subexpr<Set<Decimal>>(col_key);
            case col_type_UUID:
                return create_subexpr<Set<UUID>>(col_key);
            case col_type_ObjectId:
                return create_subexpr<Set<ObjectId>>(col_key);
            case col_type_Mixed:
                return create_subexpr<Set<Mixed>>(col_key);
            case col_type_Link:
                add(col_key);
                return create_subexpr<Link>(col_key);
            default:
                break;
        }
    }
    else if (col_key.is_list()) {
        switch (col_key.get_type()) {
            case col_type_Int:
                return create_subexpr<Lst<Int>>(col_key);
            case col_type_Bool:
                return create_subexpr<Lst<Bool>>(col_key);
            case col_type_String:
                return create_subexpr<Lst<String>>(col_key);
            case col_type_Binary:
                return create_subexpr<Lst<Binary>>(col_key);
            case col_type_Float:
                return create_subexpr<Lst<Float>>(col_key);
            case col_type_Double:
                return create_subexpr<Lst<Double>>(col_key);
            case col_type_Timestamp:
                return create_subexpr<Lst<Timestamp>>(col_key);
            case col_type_Decimal:
                return create_subexpr<Lst<Decimal>>(col_key);
            case col_type_UUID:
                return create_subexpr<Lst<UUID>>(col_key);
            case col_type_ObjectId:
                return create_subexpr<Lst<ObjectId>>(col_key);
            case col_type_Mixed:
                return create_subexpr<Lst<Mixed>>(col_key);
            case col_type_LinkList:
                add(col_key);
                return create_subexpr<Link>(col_key);
            default:
                break;
        }
    }
    else {
        if (m_comparison_type != ExpressionComparisonType::Any && list_count == 0) {
            throw InvalidQueryError(util::format("The keypath following '%1' must contain a list",
                                                 expression_cmp_type_to_str(m_comparison_type)));
        }

        switch (col_key.get_type()) {
            case col_type_Int:
                return create_subexpr<Int>(col_key);
            case col_type_Bool:
                return create_subexpr<Bool>(col_key);
            case col_type_String:
                return create_subexpr<String>(col_key);
            case col_type_Binary:
                return create_subexpr<Binary>(col_key);
            case col_type_Float:
                return create_subexpr<Float>(col_key);
            case col_type_Double:
                return create_subexpr<Double>(col_key);
            case col_type_Timestamp:
                return create_subexpr<Timestamp>(col_key);
            case col_type_Decimal:
                return create_subexpr<Decimal>(col_key);
            case col_type_UUID:
                return create_subexpr<UUID>(col_key);
            case col_type_ObjectId:
                return create_subexpr<ObjectId>(col_key);
            case col_type_Mixed:
                return create_subexpr<Mixed>(col_key);
            case col_type_Link:
                add(col_key);
                return create_subexpr<Link>(col_key);
            default:
                break;
        }
    }
    REALM_UNREACHABLE();
    return nullptr;
}

Subexpr* LinkChain::subquery(Query subquery)
{
    REALM_ASSERT(m_link_cols.size() > 0);
    auto col_key = m_link_cols.back();
    return new SubQueryCount(subquery, Columns<Link>(col_key, m_base_table, m_link_cols).link_map());
}

template <class T>
SubQuery<T> column(const Table& origin, ColKey origin_col_key, Query subquery)
{
    static_assert(std::is_same<T, BackLink>::value, "A subquery must involve a link list or backlink column");
    return SubQuery<T>(column<T>(origin, origin_col_key), std::move(subquery));
}

void PrintingVisitor::visitAnd(AndNode& node)
{
    out << "AndNode(";
    base::visitAnd(node);
    out << ")";
}
void PrintingVisitor::visitOr(OrNode& node)
{
    out << "OrNode(";
    base::visitOr(node);
    out << ")";
}
void PrintingVisitor::visitNot(NotNode& node)
{
    out << "NotNode(";
    base::visitNot(node);
    out << ")";
}
void PrintingVisitor::visitParens(ParensNode& node)
{
    out << "ParensNode(";
    base::visitParens(node);
    out << ")";
}
void PrintingVisitor::visitConstant(ConstantNode& node)
{
    out << "ConstantNode: type = " << node.type << " value = " << node.text;
}
void PrintingVisitor::visitEquality(EqualityNode& node)
{
    out << "EqualityNode(";
    base::visitEquality(node);
    out << ")";
}
void PrintingVisitor::visitRelational(RelationalNode& node)
{
    out << "RelationalNode(";
    base::visitRelational(node);
    out << ")";
}
void PrintingVisitor::visitStringOps(StringOpsNode& node)
{
    out << "StringOpsNode(";
    base::visitStringOps(node);
    out << ")";
}
void PrintingVisitor::visitTrueOrFalse(TrueOrFalseNode& node)
{
    out << "TrueOrFalseNode : value = " << node.true_or_false << " ";
}
void PrintingVisitor::visitPostOp(PostOpNode& node)
{
    out << "PostOpNode: opType = " << node.op_type << " opName = " << node.op_name << " ";
}
void PrintingVisitor::visitAggr(AggrNode& node)
{
    out << "AggrNode: type = " << node.type;
}
void PrintingVisitor::visitPath(PathNode& node)
{
    out << "PathNode: path_elems = {";
    for (auto i : node.path_elems)
        out << i << ", ";
    out << "} ";
}
void PrintingVisitor::visitListAggr(ListAggrNode& node)
{
    out << "ListAggrNode(";
    base::visitListAggr(node);
    out << ")";
}
void PrintingVisitor::visitLinkAggr(LinkAggrNode& node)
{
    out << "LinkAggrNode(";
    base::visitLinkAggr(node);
    out << ")";
}
void PrintingVisitor::visitProp(PropNode& node)
{
    out << "PropNode(identifier = " << node.identifier << " comp_type: = " << static_cast<unsigned>(node.comp_type)
        << " ";
    base::visitProp(node);
    out << ")";
}
void PrintingVisitor::visitSubquery(SubqueryNode& node)
{
    out << "SubQueryNode(";
    base::visitSubquery(node);
    out << ")";
}
void PrintingVisitor::visitDescriptor(DescriptorNode& node)
{
    out << "DescriptorNode, type = " << node.type;
}
void PrintingVisitor::visitDescriptorOrdering(DescriptorOrderingNode& node)
{
    out << "DescriptorOrderingNode(";
    base::visitDescriptorOrdering(node);
    out << ")";
}

Query QueryVisitor::visit(ParserNode& node)
{
    node.accept(*this);
    return std::move(query);
}

void QueryVisitor::visitAnd(AndNode& node)
{
    auto& atom_preds = node.atom_preds;
    if (atom_preds.size() == 1) {
        atom_preds[0]->accept(*this);
        return;
    }
    realm::Query q(drv->m_base_table);
    for (auto& it : atom_preds) {
        it->accept(*this);
        q.and_query(query);
    }
    query = q;
}

void QueryVisitor::visitOr(OrNode& node)
{
    auto& atom_preds = node.atom_preds;
    if (atom_preds.size() == 1) {
        atom_preds[0]->accept(*this);
        return;
    }
    realm::Query q(drv->m_base_table);
    q.group();
    for (auto& it : atom_preds) {
        q.Or();
        it->accept(*this);
        q.and_query(query);
    }
    q.end_group();
    query = q;
}

void QueryVisitor::visitNot(NotNode& node)
{
    node.atom_preds[0]->accept(*this);
    realm::Query q = drv->m_base_table->where();
    q.Not();
    q.and_query(query);
    query = q;
}

void QueryVisitor::visitEquality(EqualityNode& node)
{
    auto op = node.op;
    auto case_sensitive = node.case_sensitive;
    auto [left, right] = cmp(std::move(node.values));
    auto left_type = left->get_type();
    auto right_type = right->get_type();

    if (left_type == type_Link && right_type == type_TypedLink && right->has_constant_evaluation()) {
        if (auto link_column = dynamic_cast<const Columns<Link>*>(left.get())) {
            if (right->get_mixed().is_null()) {
                right_type = ColumnTypeTraits<realm::null>::id;
                right = std::make_unique<Value<realm::null>>();
            }
            else {
                auto left_dest_table_key = link_column->link_map().get_target_table()->get_key();
                auto right_table_key = right->get_mixed().get_link().get_table_key();
                auto right_obj_key = right->get_mixed().get_link().get_obj_key();
                if (left_dest_table_key == right_table_key) {
                    right = std::make_unique<Value<ObjKey>>(right_obj_key);
                    right_type = type_Link;
                }
                else {
                    const Group* g = drv->m_base_table->get_parent_group();
                    throw std::invalid_argument(util::format(
                        "The relationship '%1' which links to type '%2' cannot be compared to an argument of type %3",
                        link_column->link_map().description(drv->m_serializer_state),
                        drv->get_printable_name(link_column->link_map().get_target_table()->get_name()),
                        print_pretty_objlink(right->get_mixed().get_link(), g, drv)));
                }
            }
        }
    }

    if (left_type.is_valid() && right_type.is_valid() && !Mixed::data_types_are_comparable(left_type, right_type)) {
        throw InvalidQueryError(util::format("Unsupported comparison between type '%1' and type '%2'",
                                             get_data_type_name(left_type), get_data_type_name(right_type)));
    }
    if (left_type == type_TypeOfValue || right_type == type_TypeOfValue) {
        if (left_type != right_type) {
            throw InvalidQueryArgError(
                util::format("Unsupported comparison between @type and raw value: '%1' and '%2'",
                             get_data_type_name(left_type), get_data_type_name(right_type)));
        }
    }

    if (op == CompareNode::IN) {
        Subexpr* r = right.get();
        if (!r->has_multiple_values()) {
            throw InvalidQueryArgError("The keypath following 'IN' must contain a list");
        }
    }

    const ObjPropertyBase* prop = dynamic_cast<const ObjPropertyBase*>(left.get());
    if (right->has_constant_evaluation() && (left_type == right_type || left_type == type_Mixed)) {
        Mixed val = right->get_mixed();
        if (prop && !prop->links_exist()) {
            auto col_key = prop->column_key();
            if (val.is_null()) {
                switch (op) {
                    case CompareNode::EQUAL:
                    case CompareNode::IN:
                        query = drv->m_base_table->where().equal(col_key, realm::null());
                        return;
                    case CompareNode::NOT_EQUAL:
                        query = drv->m_base_table->where().not_equal(col_key, realm::null());
                        return;
                }
            }
            switch (left->get_type()) {
                case type_Int:
                    query = drv->simple_query(op, col_key, val.get_int());
                    return;
                case type_Bool:
                    query = drv->simple_query(op, col_key, val.get_bool());
                    return;
                case type_String:
                    query = drv->simple_query(op, col_key, val.get_string(), case_sensitive);
                    return;
                case type_Binary:
                    query = drv->simple_query(op, col_key, val.get_binary(), case_sensitive);
                    return;
                case type_Timestamp:
                    query = drv->simple_query(op, col_key, val.get<Timestamp>());
                    return;
                case type_Float:
                    query = drv->simple_query(op, col_key, val.get_float());
                    return;
                case type_Double:
                    query = drv->simple_query(op, col_key, val.get_double());
                    return;
                case type_Decimal:
                    query = drv->simple_query(op, col_key, val.get<Decimal128>());
                    return;
                case type_ObjectId:
                    query = drv->simple_query(op, col_key, val.get<ObjectId>());
                    return;
                case type_UUID:
                    query = drv->simple_query(op, col_key, val.get<UUID>());
                    return;
                case type_Mixed:
                    query = drv->simple_query(op, col_key, val, case_sensitive);
                    return;
                default:
                    break;
            }
        }
        else if (left_type == type_Link) {
            auto link_column = dynamic_cast<const Columns<Link>*>(left.get());
            if (link_column && link_column->link_map().get_nb_hops() == 1 &&
                link_column->get_comparison_type() == ExpressionComparisonType::Any) {
                // We can use equal/not_equal and get a LinksToNode based query
                if (op == CompareNode::EQUAL) {
                    query = drv->m_base_table->where().equal(link_column->link_map().get_first_column_key(), val);
                    return;
                }
                else if (op == CompareNode::NOT_EQUAL) {
                    query = drv->m_base_table->where().not_equal(link_column->link_map().get_first_column_key(), val);
                    return;
                }
            }
        }
    }
    if (case_sensitive) {
        switch (op) {
            case CompareNode::EQUAL:
            case CompareNode::IN:
                query =
                    realm::Query(std::unique_ptr<Expression>(new Compare<Equal>(std::move(right), std::move(left))));
                return;
            case CompareNode::NOT_EQUAL:
                query = realm::Query(
                    std::unique_ptr<Expression>(new Compare<NotEqual>(std::move(right), std::move(left))));
                return;
        }
    }
    else {
        verify_only_string_types(right_type, opstr[op] + "[c]");
        switch (op) {
            case CompareNode::EQUAL:
            case CompareNode::IN:
                query = Query(std::unique_ptr<Expression>(new Compare<EqualIns>(std::move(right), std::move(left))));
                return;
            case CompareNode::NOT_EQUAL:
                query =
                    Query(std::unique_ptr<Expression>(new Compare<NotEqualIns>(std::move(right), std::move(left))));
                return;
        }
    }
    query = {};
}

void QueryVisitor::visitRelational(RelationalNode& node)
{
    auto op = node.op;
    auto [left, right] = cmp(std::move(node.values));

    auto left_type = left->get_type();
    auto right_type = right->get_type();
    const bool right_type_is_null = right->has_constant_evaluation() && right->get_mixed().is_null();
    const bool left_type_is_null = left->has_constant_evaluation() && left->get_mixed().is_null();
    REALM_ASSERT(!(left_type_is_null && right_type_is_null));

    if (left_type == type_Link || left_type == type_TypeOfValue) {
        throw InvalidQueryError(util::format(
            "Unsupported operator %1 in query. Only equal (==) and not equal (!=) are supported for this type.",
            opstr[op]));
    }

    if (!(left_type_is_null || right_type_is_null) && (!left_type.is_valid() || !right_type.is_valid() ||
                                                       !Mixed::data_types_are_comparable(left_type, right_type))) {
        throw InvalidQueryError(util::format("Unsupported comparison between type '%1' and type '%2'",
                                             get_data_type_name(left_type), get_data_type_name(right_type)));
    }

    const ObjPropertyBase* prop = dynamic_cast<const ObjPropertyBase*>(left.get());
    if (prop && !prop->links_exist() && right->has_constant_evaluation() &&
        (left_type == right_type || left_type == type_Mixed)) {
        auto col_key = prop->column_key();
        switch (left->get_type()) {
            case type_Int:
                query = drv->simple_query(op, col_key, right->get_mixed().get_int());
                return;
            case type_Bool:
                break;
            case type_String:
                break;
            case type_Binary:
                break;
            case type_Timestamp:
                query = drv->simple_query(op, col_key, right->get_mixed().get<Timestamp>());
                return;
            case type_Float:
                query = drv->simple_query(op, col_key, right->get_mixed().get_float());
                return;
            case type_Double:
                query = drv->simple_query(op, col_key, right->get_mixed().get_double());
                return;
            case type_Decimal:
                query = drv->simple_query(op, col_key, right->get_mixed().get<Decimal128>());
                return;
            case type_ObjectId:
                query = drv->simple_query(op, col_key, right->get_mixed().get<ObjectId>());
                return;
            case type_UUID:
                query = drv->simple_query(op, col_key, right->get_mixed().get<UUID>());
                return;
            case type_Mixed:
                query = drv->simple_query(op, col_key, right->get_mixed());
                return;
            default:
                break;
        }
    }
    switch (op) {
        case CompareNode::GREATER:
            query = realm::Query(std::unique_ptr<Expression>(new Compare<Less>(std::move(right), std::move(left))));
            return;
        case CompareNode::LESS:
            query =
                realm::Query(std::unique_ptr<Expression>(new Compare<Greater>(std::move(right), std::move(left))));
            return;
        case CompareNode::GREATER_EQUAL:
            query =
                realm::Query(std::unique_ptr<Expression>(new Compare<LessEqual>(std::move(right), std::move(left))));
            return;
        case CompareNode::LESS_EQUAL:
            query = realm::Query(
                std::unique_ptr<Expression>(new Compare<GreaterEqual>(std::move(right), std::move(left))));
            return;
    }
    query = {};
}

void QueryVisitor::visitStringOps(StringOpsNode& node)
{
    auto op = node.op;
    auto case_sensitive = node.case_sensitive;
    auto [left, right] = drv->cmp(std::move(node.values));

    auto left_type = left->get_type();
    auto right_type = right->get_type();
    const ObjPropertyBase* prop = dynamic_cast<const ObjPropertyBase*>(left.get());

    verify_only_string_types(right_type, opstr[op]);

    if (prop && !prop->links_exist() && right->has_constant_evaluation() &&
        (left_type == right_type || left_type == type_Mixed)) {
        auto col_key = prop->column_key();
        if (right_type == type_String) {
            StringData val = right->get_mixed().get_string();

            switch (op) {
                case CompareNode::BEGINSWITH:
                    query = drv->m_base_table->where().begins_with(col_key, val, case_sensitive);
                    return;
                case CompareNode::ENDSWITH:
                    query = drv->m_base_table->where().ends_with(col_key, val, case_sensitive);
                    return;
                case CompareNode::CONTAINS:
                    query = drv->m_base_table->where().contains(col_key, val, case_sensitive);
                    return;
                case CompareNode::LIKE:
                    query = drv->m_base_table->where().like(col_key, val, case_sensitive);
                    return;
            }
        }
        else if (right_type == type_Binary) {
            BinaryData val = right->get_mixed().get_binary();

            switch (op) {
                case CompareNode::BEGINSWITH:
                    query = drv->m_base_table->where().begins_with(col_key, val, case_sensitive);
                    return;
                case CompareNode::ENDSWITH:
                    query = drv->m_base_table->where().ends_with(col_key, val, case_sensitive);
                    return;
                case CompareNode::CONTAINS:
                    query = drv->m_base_table->where().contains(col_key, val, case_sensitive);
                    return;
                case CompareNode::LIKE:
                    query = drv->m_base_table->where().like(col_key, val, case_sensitive);
                    return;
            }
        }
    }

    if (case_sensitive) {
        switch (op) {
            case CompareNode::BEGINSWITH:
                query =
                    Query(std::unique_ptr<Expression>(new Compare<BeginsWith>(std::move(right), std::move(left))));
                return;
            case CompareNode::ENDSWITH:
                query = Query(std::unique_ptr<Expression>(new Compare<EndsWith>(std::move(right), std::move(left))));
                return;
            case CompareNode::CONTAINS:
                query = Query(std::unique_ptr<Expression>(new Compare<Contains>(std::move(right), std::move(left))));
                return;
            case CompareNode::LIKE:
                query = Query(std::unique_ptr<Expression>(new Compare<Like>(std::move(right), std::move(left))));
                return;
        }
    }
    else {
        switch (op) {
            case CompareNode::BEGINSWITH:
                query =
                    Query(std::unique_ptr<Expression>(new Compare<BeginsWithIns>(std::move(right), std::move(left))));
                return;
            case CompareNode::ENDSWITH:
                query =
                    Query(std::unique_ptr<Expression>(new Compare<EndsWithIns>(std::move(right), std::move(left))));
                return;
            case CompareNode::CONTAINS:
                query =
                    Query(std::unique_ptr<Expression>(new Compare<ContainsIns>(std::move(right), std::move(left))));
                return;
            case CompareNode::LIKE:
                query = Query(std::unique_ptr<Expression>(new Compare<LikeIns>(std::move(right), std::move(left))));
                return;
        }
    }
    query = {};
}
void QueryVisitor::visitTrueOrFalse(TrueOrFalseNode& node)
{
    auto true_or_false = node.true_or_false;
    Query q = drv->m_base_table->where();
    if (true_or_false) {
        q.and_query(std::unique_ptr<realm::Expression>(new TrueExpression));
    }
    else {
        q.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
    }
    query = q;
}

std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>>
QueryVisitor::cmp(std::vector<std::unique_ptr<ValueNode>>&& values)
{
    std::unique_ptr<Subexpr> left;
    std::unique_ptr<Subexpr> right;

    auto left_constant = std::move(values[0]->constant);
    auto right_constant = std::move(values[1]->constant);
    auto left_prop = std::move(values[0]->prop);
    auto right_prop = std::move(values[1]->prop);

    if (left_constant && right_constant) {
        throw InvalidQueryError("Cannot compare two constants");
    }
    if (right_constant) {
        // Take left first - it cannot be a constant
        left = SubexprVisitor(drv).visit(*left_prop);
        right = SubexprVisitor(drv, left->get_type()).visit(*right_constant);
        verify_conditions(left.get(), right.get(), drv->m_serializer_state);
    }
    else {
        right = SubexprVisitor(drv).visit(*right_prop);
        if (left_constant) {
            left = SubexprVisitor(drv, right->get_type()).visit(*left_constant);
        }
        else {
            left = SubexprVisitor(drv).visit(*left_prop);
        }
        verify_conditions(right.get(), left.get(), drv->m_serializer_state);
    }
    return {std::move(left), std::move(right)};
}

std::unique_ptr<DescriptorOrdering> QueryVisitor::getDescriptorOrdering(std::unique_ptr<DescriptorOrderingNode>& node)
{
    auto orderings = std::move(node->orderings);
    auto target = drv->m_base_table;
    std::unique_ptr<DescriptorOrdering> ordering;
    for (auto& cur_ordering : orderings) {
        if (!ordering)
            ordering = std::make_unique<DescriptorOrdering>();
        if (cur_ordering->get_type() == DescriptorNode::LIMIT) {
            ordering->append_limit(LimitDescriptor(cur_ordering->limit));
        }
        else {
            bool is_distinct = cur_ordering->get_type() == DescriptorNode::DISTINCT;
            std::vector<std::vector<ColKey>> property_columns;
            for (auto& col_names : cur_ordering->columns) {
                std::vector<ColKey> columns;
                LinkChain link_chain(target);
                for (size_t ndx_in_path = 0; ndx_in_path < col_names.size(); ++ndx_in_path) {
                    std::string path_elem = drv->translate(link_chain, col_names[ndx_in_path]);
                    ColKey col_key = link_chain.get_current_table()->get_column_key(path_elem);
                    if (!col_key) {
                        throw InvalidQueryError(
                            util::format("No property '%1' found on object type '%2' specified in '%3' clause",
                                         col_names[ndx_in_path],
                                         drv->get_printable_name(link_chain.get_current_table()->get_name()),
                                         is_distinct ? "distinct" : "sort"));
                    }
                    columns.push_back(col_key);
                    if (ndx_in_path < col_names.size() - 1) {
                        link_chain.link(col_key);
                    }
                }
                property_columns.push_back(columns);
            }

            if (is_distinct) {
                ordering->append_distinct(DistinctDescriptor(property_columns));
            }
            else {
                ordering->append_sort(SortDescriptor(property_columns, cur_ordering->ascending),
                                      cur_ordering->merge_mode);
            }
        }
    }
    return ordering;
}

std::unique_ptr<realm::Subexpr> SubexprVisitor::visit(ParserNode& node)
{
    // base::visit(*node);
    node.accept(*this);
    return std::move(subexpr);
}

void SubexprVisitor::visitConstant(ConstantNode& node)
{
    auto text = node.text;
    auto type = node.type;
    auto hint = this->t;
    Subexpr* ret = nullptr;
    std::string explain_value_message = text;
    switch (type) {
        case ConstantNode::Type::NUMBER: {
            if (hint == type_Decimal) {
                ret = new Value<Decimal128>(Decimal128(text));
            }
            else {
                ret = new Value<int64_t>(strtoll(text.c_str(), nullptr, 0));
            }
            break;
        }
        case ConstantNode::Type::FLOAT: {
            switch (hint) {
                case type_Float: {
                    ret = new Value<float>(strtof(text.c_str(), nullptr));
                    break;
                }
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128(text));
                    break;
                default:
                    ret = new Value<double>(strtod(text.c_str(), nullptr));
                    break;
            }
            break;
        }
        case ConstantNode::Type::INFINITY_VAL: {
            bool negative = text[0] == '-';
            switch (hint) {
                case type_Float: {
                    auto inf = std::numeric_limits<float>::infinity();
                    ret = new Value<float>(negative ? -inf : inf);
                    break;
                }
                case type_Double: {
                    auto inf = std::numeric_limits<double>::infinity();
                    ret = new Value<double>(negative ? -inf : inf);
                    break;
                }
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128(text));
                    break;
                default:
                    throw InvalidQueryError(util::format("Infinity not supported for %1", get_data_type_name(hint)));
                    break;
            }
            break;
        }
        case ConstantNode::Type::NAN_VAL: {
            switch (hint) {
                case type_Float:
                    ret = new Value<float>(type_punning<float>(0x7fc00000));
                    break;
                case type_Double:
                    ret = new Value<double>(type_punning<double>(0x7ff8000000000000));
                    break;
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128::nan("0"));
                    break;
                default:
                    REALM_UNREACHABLE();
                    break;
            }
            break;
        }
        case ConstantNode::Type::STRING: {
            std::string str = text.substr(1, text.size() - 2);
            switch (hint) {
                case type_Int:
                    ret = new Value<int64_t>(string_to<int64_t>(str));
                    break;
                case type_Float:
                    ret = new Value<float>(string_to<float>(str));
                    break;
                case type_Double:
                    ret = new Value<double>(string_to<double>(str));
                    break;
                case type_Decimal:
                    ret = new Value<Decimal128>(Decimal128(str.c_str()));
                    break;
                default:
                    if (hint == type_TypeOfValue) {
                        try {
                            ret = new Value<TypeOfValue>(TypeOfValue(str));
                        }
                        catch (const std::runtime_error& e) {
                            throw InvalidQueryArgError(e.what());
                        }
                    }
                    else {
                        ret = new ConstantStringValue(str);
                    }
                    break;
            }
            break;
        }
        case ConstantNode::Type::BASE64: {
            const size_t encoded_size = text.size() - 5;
            size_t buffer_size = util::base64_decoded_size(encoded_size);
            drv->m_args.buffer_space.push_back({});
            auto& decode_buffer = drv->m_args.buffer_space.back();
            decode_buffer.resize(buffer_size);
            StringData window(text.c_str() + 4, encoded_size);
            util::Optional<size_t> decoded_size = util::base64_decode(window, decode_buffer.data(), buffer_size);
            if (!decoded_size) {
                throw SyntaxError("Invalid base64 value");
            }
            REALM_ASSERT_DEBUG_EX(*decoded_size <= encoded_size, *decoded_size, encoded_size);
            decode_buffer.resize(*decoded_size); // truncate

            if (hint == type_String) {
                ret = new ConstantStringValue(StringData(decode_buffer.data(), decode_buffer.size()));
            }
            if (hint == type_Binary) {
                ret = new Value<BinaryData>(BinaryData(decode_buffer.data(), decode_buffer.size()));
            }
            if (hint == type_Mixed) {
                ret = new Value<BinaryData>(BinaryData(decode_buffer.data(), decode_buffer.size()));
            }
            break;
        }
        case ConstantNode::Type::TIMESTAMP: {
            auto s = text;
            int64_t seconds;
            int32_t nanoseconds;
            if (s[0] == 'T') {
                size_t colon_pos = s.find(":");
                std::string s1 = s.substr(1, colon_pos - 1);
                std::string s2 = s.substr(colon_pos + 1);
                seconds = strtol(s1.c_str(), nullptr, 0);
                nanoseconds = int32_t(strtol(s2.c_str(), nullptr, 0));
            }
            else {
                // readable format YYYY-MM-DD-HH:MM:SS:NANOS nanos optional
                struct tm tmp = tm();
                char sep = s.find("@") < s.size() ? '@' : 'T';
                std::string fmt = "%d-%d-%d"s + sep + "%d:%d:%d:%d"s;
                int cnt = sscanf(s.c_str(), fmt.c_str(), &tmp.tm_year, &tmp.tm_mon, &tmp.tm_mday, &tmp.tm_hour,
                                 &tmp.tm_min, &tmp.tm_sec, &nanoseconds);
                REALM_ASSERT(cnt >= 6);
                tmp.tm_year -= 1900; // epoch offset (see man mktime)
                tmp.tm_mon -= 1;     // converts from 1-12 to 0-11

                if (tmp.tm_year < 0) {
                    // platform timegm functions do not throw errors, they return -1 which is also a valid time
                    throw InvalidQueryError("Conversion of dates before 1900 is not supported.");
                }

                seconds = platform_timegm(tmp); // UTC time
                if (cnt == 6) {
                    nanoseconds = 0;
                }
                if (nanoseconds < 0) {
                    throw SyntaxError("The nanoseconds of a Timestamp cannot be negative.");
                }
                if (seconds < 0) { // seconds determines the sign of the nanoseconds part
                    nanoseconds *= -1;
                }
            }
            ret = new Value<Timestamp>(get_timestamp_if_valid(seconds, nanoseconds));
            break;
        }
        case ConstantNode::Type::UUID_T:
            ret = new Value<UUID>(UUID(text.substr(5, text.size() - 6)));
            break;
        case ConstantNode::Type::OID:
            ret = new Value<ObjectId>(ObjectId(text.substr(4, text.size() - 5).c_str()));
            break;
        case ConstantNode::Type::LINK: {
            ret = new Value<ObjKey>(ObjKey(strtol(text.substr(1, text.size() - 1).c_str(), nullptr, 0)));
            break;
        }
        case ConstantNode::Type::TYPED_LINK: {
            size_t colon_pos = text.find(":");
            auto table_key_val = uint32_t(strtol(text.substr(1, colon_pos - 1).c_str(), nullptr, 0));
            auto obj_key_val = strtol(text.substr(colon_pos + 1).c_str(), nullptr, 0);
            ret = new Value<ObjLink>(ObjLink(TableKey(table_key_val), ObjKey(obj_key_val)));
            break;
        }
        case ConstantNode::Type::NULL_VAL:
            if (hint == type_String) {
                ret = new ConstantStringValue(StringData()); // Null string
            }
            else if (hint == type_Binary) {
                ret = new Value<Binary>(BinaryData()); // Null string
            }
            else {
                ret = new Value<null>(realm::null());
            }
            break;
        case ConstantNode::Type::TRUE:
            ret = new Value<Bool>(true);
            break;
        case ConstantNode::Type::FALSE:
            ret = new Value<Bool>(false);
            break;
        case ConstantNode::Type::ARG: {
            size_t arg_no = size_t(strtol(text.substr(1).c_str(), nullptr, 10));
            if (drv->m_args.is_argument_null(arg_no)) {
                explain_value_message = util::format("argument '%1' which is NULL", explain_value_message);
                ret = new Value<null>(realm::null());
            }
            else {
                auto type = drv->m_args.type_for_argument(arg_no);
                explain_value_message =
                    util::format("argument %1 of type '%2'", explain_value_message, get_data_type_name(type));
                switch (type) {
                    case type_Int:
                        ret = new Value<int64_t>(drv->m_args.long_for_argument(arg_no));
                        break;
                    case type_String:
                        ret = new ConstantStringValue(drv->m_args.string_for_argument(arg_no));
                        break;
                    case type_Binary:
                        ret = new ConstantBinaryValue(drv->m_args.binary_for_argument(arg_no));
                        break;
                    case type_Bool:
                        ret = new Value<Bool>(drv->m_args.bool_for_argument(arg_no));
                        break;
                    case type_Float:
                        ret = new Value<float>(drv->m_args.float_for_argument(arg_no));
                        break;
                    case type_Double: {
                        // In realm-js all number type arguments are returned as double. If we don't cast to the
                        // expected type, we would in many cases miss the option to use the optimized query node
                        // instead of the general Compare class.
                        double val = drv->m_args.double_for_argument(arg_no);
                        switch (hint) {
                            case type_Int:
                            case type_Bool: {
                                int64_t int_val = int64_t(val);
                                // Only return an integer if it precisely represents val
                                if (double(int_val) == val)
                                    ret = new Value<int64_t>(int_val);
                                else
                                    ret = new Value<double>(val);
                                break;
                            }
                            case type_Float:
                                ret = new Value<float>(float(val));
                                break;
                            default:
                                ret = new Value<double>(val);
                                break;
                        }
                        break;
                    }
                    case type_Timestamp: {
                        try {
                            ret = new Value<Timestamp>(drv->m_args.timestamp_for_argument(arg_no));
                        }
                        catch (const std::exception&) {
                            ret = new Value<ObjectId>(drv->m_args.objectid_for_argument(arg_no));
                        }
                        break;
                    }
                    case type_ObjectId: {
                        try {
                            ret = new Value<ObjectId>(drv->m_args.objectid_for_argument(arg_no));
                        }
                        catch (const std::exception&) {
                            ret = new Value<Timestamp>(drv->m_args.timestamp_for_argument(arg_no));
                        }
                        break;
                    }
                    case type_Decimal:
                        ret = new Value<Decimal128>(drv->m_args.decimal128_for_argument(arg_no));
                        break;
                    case type_UUID:
                        ret = new Value<UUID>(drv->m_args.uuid_for_argument(arg_no));
                        break;
                    case type_Link:
                        ret = new Value<ObjKey>(drv->m_args.object_index_for_argument(arg_no));
                        break;
                    case type_TypedLink:
                        if (hint == type_Mixed || hint == type_Link || hint == type_TypedLink) {
                            ret = new Value<ObjLink>(drv->m_args.objlink_for_argument(arg_no));
                            break;
                        }
                        explain_value_message =
                            util::format("%1 which links to %2", explain_value_message,
                                         print_pretty_objlink(drv->m_args.objlink_for_argument(arg_no),
                                                              drv->m_base_table->get_parent_group(), drv));
                        break;
                    default:
                        break;
                }
            }
            break;
        }
    }
    if (!ret) {
        throw InvalidQueryError(
            util::format("Unsupported comparison between property of type '%1' and constant value: %2",
                         get_data_type_name(hint), explain_value_message));
    }
    subexpr = std::unique_ptr<Subexpr>{ret};
}

void SubexprVisitor::visitPostOp(PostOpNode& node)
{
    auto op_type = node.op_type;
    auto op_name = node.op_name;
    if (op_type == PostOpNode::SIZE) {
        if (auto s = dynamic_cast<Columns<Link>*>(subexpr.get())) {
            subexpr = s->count().clone();
            return;
        }
        if (auto s = dynamic_cast<ColumnListBase*>(subexpr.get())) {
            subexpr = s->size().clone();
            return;
        }
        if (auto s = dynamic_cast<Columns<StringData>*>(subexpr.get())) {
            subexpr = s->size().clone();
            return;
        }
        if (auto s = dynamic_cast<Columns<BinaryData>*>(subexpr.get())) {
            subexpr = s->size().clone();
            return;
        }
    }
    else if (op_type == PostOpNode::TYPE) {
        if (auto s = dynamic_cast<Columns<Mixed>*>(subexpr.get())) {
            subexpr = s->type_of_value().clone();
            return;
        }
        if (auto s = dynamic_cast<ColumnsCollection<Mixed>*>(subexpr.get())) {
            subexpr = s->type_of_value().clone();
            return;
        }
        if (auto s = dynamic_cast<ObjPropertyBase*>(subexpr.get())) {
            subexpr = Value<TypeOfValue>(TypeOfValue(s->column_key())).clone();
            return;
        }
        if (dynamic_cast<Columns<Link>*>(subexpr.get())) {
            subexpr = Value<TypeOfValue>(TypeOfValue(TypeOfValue::Attribute::ObjectLink)).clone();
            return;
        }
    }

    if (subexpr) {
        throw InvalidQueryError(util::format("Operation '%1' is not supported on property of type '%2'", op_name,
                                             get_data_type_name(DataType(subexpr->get_type()))));
    }
    REALM_UNREACHABLE();
}

void SubexprVisitor::visitAggr(AggrNode& node)
{
    auto type = node.type;
    std::unique_ptr<Subexpr> agg;
    if (auto list_prop = dynamic_cast<ColumnListBase*>(subexpr.get())) {
        switch (type) {
            case AggrNode::MAX:
                agg = list_prop->max_of();
                break;
            case AggrNode::MIN:
                agg = list_prop->min_of();
                break;
            case AggrNode::SUM:
                agg = list_prop->sum_of();
                break;
            case AggrNode::AVG:
                agg = list_prop->avg_of();
                break;
        }
    }
    else if (auto prop = dynamic_cast<SubColumnBase*>(subexpr.get())) {
        switch (type) {
            case AggrNode::MAX:
                agg = prop->max_of();
                break;
            case AggrNode::MIN:
                agg = prop->min_of();
                break;
            case AggrNode::SUM:
                agg = prop->sum_of();
                break;
            case AggrNode::AVG:
                agg = prop->avg_of();
                break;
        }
    }
    if (!agg) {
        throw InvalidQueryError(
            util::format("Cannot use aggregate '%1' for this type of property", agg_op_type_to_str(type)));
    }
    subexpr = std::move(agg);
}

void SubexprVisitor::visitListAggr(ListAggrNode& node)
{
    LinkChain link_chain = getLinkChain(*node.path);
    subexpr = std::unique_ptr<Subexpr>(drv->column(link_chain, node.identifier));
    visitAggr(*node.aggr_op);
}

void SubexprVisitor::visitLinkAggr(LinkAggrNode& node)
{
    auto path = std::move(node.path);
    auto prop = node.prop;
    auto link = node.link;
    auto aggr_op = std::move(node.aggr_op);
    LinkChain link_chain = getLinkChain(*path);
    auto subexprtmp = std::unique_ptr<Subexpr>(drv->column(link_chain, link));
    auto link_prop = dynamic_cast<Columns<Link>*>(subexprtmp.get());
    if (!link_prop) {
        throw InvalidQueryError(util::format("Operation '%1' cannot apply to property '%2' because it is not a list",
                                             agg_op_type_to_str(aggr_op->type), link));
    }
    prop = drv->translate(link_chain, prop);
    auto col_key = link_chain.get_current_table()->get_column_key(prop);

    std::unique_ptr<Subexpr> sub_column;
    switch (col_key.get_type()) {
        case col_type_Int:
            sub_column = link_prop->column<Int>(col_key).clone();
            break;
        case col_type_Float:
            sub_column = link_prop->column<float>(col_key).clone();
            break;
        case col_type_Double:
            sub_column = link_prop->column<double>(col_key).clone();
            break;
        case col_type_Decimal:
            sub_column = link_prop->column<Decimal>(col_key).clone();
            break;
        case col_type_Timestamp:
            sub_column = link_prop->column<Timestamp>(col_key).clone();
            break;
        default:
            throw InvalidQueryError(util::format("collection aggregate not supported for type '%1'",
                                                 get_data_type_name(DataType(col_key.get_type()))));
    }
    subexpr = std::move(sub_column);
    visitAggr(*aggr_op);
}

void SubexprVisitor::visitProp(PropNode& node)
{
    auto identifier = node.identifier;
    auto path = std::move(node.path);
    auto comp_type = node.comp_type;
    auto index = std::move(node.index);
    auto post_op = std::move(node.post_op);
    bool is_keys = false;
    if (identifier[0] == '@') {
        if (identifier == "@values") {
            identifier = path->path_elems.back();
            path->path_elems.pop_back();
        }
        else if (identifier == "@keys") {
            identifier = path->path_elems.back();
            path->path_elems.pop_back();
            is_keys = true;
        }
        else if (identifier == "@links") {
            // This is a backlink aggregate query
            LinkChain link_chain = getLinkChain(*path, comp_type);
            auto sub = link_chain.get_backlink_count<Int>();
            subexpr = sub.clone();
            return;
        }
    }
    try {
        LinkChain link_chain = getLinkChain(*path, comp_type);
        std::unique_ptr<Subexpr> test{drv->column(link_chain, identifier)};
        // std::unique_ptr<Subexpr> test = std::unique_ptr<Subexpr>(drv->column(link_chain, identifier));
        if (index) {
            if (auto s = dynamic_cast<Columns<Dictionary>*>(test.get())) {
                auto t = s->get_type();
                // SubexprVisitor(drv, t).visit(index);
                auto idx = SubexprVisitor(drv, t).visit(*index);
                Mixed key = idx->get_mixed();
                test = s->key(key).clone();
            }
        }
        if (is_keys) {
            if (auto s = dynamic_cast<Columns<Dictionary>*>(test.get())) {
                test = std::make_unique<ColumnDictionaryKeys>(*s);
            }
        }
        subexpr = std::move(test);
        if (post_op) {
            visitPostOp(*post_op);
        }
        return;
    }
    catch (const std::runtime_error& e) {
        // Is 'identifier' perhaps length operator?
        if (!post_op && is_length_suffix(identifier) && path->path_elems.size() > 0) {
            // If 'length' is the operator, the last id in the path must be the name
            // of a list property
            auto prop = std::move(path->path_elems.back());
            path->path_elems.pop_back();
            std::unique_ptr<Subexpr> column{getLinkChain(*path, comp_type).column(prop)};
            if (auto list = dynamic_cast<ColumnListBase*>(column.get())) {
                if (auto length_expr = list->get_element_length()) {
                    subexpr = std::move(length_expr);
                    return;
                }
            }
        }
        throw InvalidQueryError(e.what());
    }
    REALM_UNREACHABLE();
}

void SubexprVisitor::visitSubquery(SubqueryNode& node)
{
    auto variable_name = node.variable_name;
    auto prop = std::move(node.prop);
    auto subquery = std::move(node.subquery);
    if (variable_name.size() < 2 || variable_name[0] != '$') {
        throw SyntaxError(util::format("The subquery variable '%1' is invalid. The variable must start with "
                                       "'$' and cannot be empty; for example '$x'.",
                                       variable_name));
    }
    LinkChain lc = getLinkChain(*prop->path, prop->comp_type);
    prop->identifier = drv->translate(lc, prop->identifier);

    if (prop->identifier.find("@links") == 0) {
        drv->backlink(lc, prop->identifier);
    }
    else {
        ColKey col_key = lc.get_current_table()->get_column_key(prop->identifier);
        if (col_key.is_list() && col_key.get_type() != col_type_LinkList) {
            throw InvalidQueryError(util::format(
                "A subquery can not operate on a list of primitive values (property '%1')", prop->identifier));
        }
        if (col_key.get_type() != col_type_LinkList) {
            throw InvalidQueryError(util::format("A subquery must operate on a list property, but '%1' is type '%2'",
                                                 prop->identifier,
                                                 realm::get_data_type_name(DataType(col_key.get_type()))));
        }
        lc.link(prop->identifier);
    }
    TableRef previous_table = drv->m_base_table;
    drv->m_base_table = lc.get_current_table().cast_away_const();
    bool did_add = drv->m_mapping.add_mapping(drv->m_base_table, variable_name, "");
    if (!did_add) {
        throw InvalidQueryError(util::format("Unable to create a subquery expression with variable '%1' since an "
                                             "identical variable already exists in this context",
                                             variable_name));
    }
    Query query = QueryVisitor(drv).visit(*subquery);
    drv->m_mapping.remove_mapping(drv->m_base_table, variable_name);
    drv->m_base_table = previous_table;

    subexpr = std::unique_ptr<Subexpr>(lc.subquery(query));
}


LinkChain SubexprVisitor::getLinkChain(PathNode& node, ExpressionComparisonType comp_type)
{
    LinkChain link_chain = LinkChain(drv->m_base_table, comp_type);
    auto path_elems = node.path_elems;
    for (std::string path_elem : path_elems) {
        path_elem = drv->translate(link_chain, path_elem);
        if (path_elem.find("@links.") == 0) {
            drv->backlink(link_chain, path_elem);
        }
        else if (path_elem == "@values") {
            if (!link_chain.get_current_col().is_dictionary()) {
                throw InvalidQueryError("@values only allowed on dictionaries");
            }
            continue;
        }
        else if (path_elem.empty()) {
            continue; // this element has been removed, this happens in subqueries
        }
        else {
            try {
                link_chain.link(path_elem);
            }
            // I case of exception, we have to throw InvalidQueryError
            catch (const std::runtime_error& e) {
                auto str = e.what();
                StringData table_name = drv->get_printable_name(link_chain.get_current_table()->get_name());
                if (strstr(str, "no property")) {
                    throw InvalidQueryError(util::format("'%1' has no property: '%2'", table_name, path_elem));
                }
                else {
                    throw InvalidQueryError(
                        util::format("Property '%1' in '%2' is not an Object", path_elem, table_name));
                }
            }
        }
    }
    return link_chain;
}


using json = nlohmann::json;
Query JsonQueryParser::query_from_json(TableRef table, json json)
{
    auto and_node = std::make_unique<AndNode>();
    auto don = std::make_unique<DescriptorOrderingNode>();
    for (auto predicate : json["whereClauses"]) {
        build_pred(predicate["expression"], and_node->atom_preds);
    }
    for (auto ordering : json["orderingClauses"]) {
        build_descriptor(ordering, don->orderings);
    }
    std::unique_ptr<Arguments> no_arguments(new NoArguments());
    ParserDriver driver(table, *no_arguments, KeyPathMapping());
    std::unique_ptr<DescriptorOrdering> order = QueryVisitor(&driver).getDescriptorOrdering(don);
    return QueryVisitor(&driver).visit(*and_node).set_ordering(std::move(order));
}


void JsonQueryParser::build_pred(json fragment, std::vector<std::unique_ptr<AtomPredNode>>& preds)
{
    if (fragment["kind"] == "and") {
        auto and_node = std::make_unique<AndNode>();
        build_pred(fragment["left"], and_node->atom_preds);
        build_pred(fragment["right"], and_node->atom_preds);
        preds.emplace_back(std::move(and_node));
    }
    else if (fragment["kind"] == "or") {
        auto or_node = std::make_unique<OrNode>();
        build_pred(fragment["left"], or_node->atom_preds);
        build_pred(fragment["right"], or_node->atom_preds);
        preds.emplace_back(std::move(or_node));
    }
    else if (fragment["kind"] == "not") {
        auto not_node = std::make_unique<NotNode>();
        build_pred(fragment["expression"], not_node->atom_preds);
        preds.emplace_back(std::move(not_node));
    }
    else {
        build_compare(fragment, preds);
    }
}

void JsonQueryParser::build_descriptor(json fragment, std::vector<std::unique_ptr<DescriptorNode>>& orderings)
{
    auto ordering = std::make_unique<DescriptorNode>(DescriptorNode::SORT, SortDescriptor::MergeMode::append);
    auto empty_path = std::make_unique<PathNode>();
    ordering->add(empty_path->path_elems, fragment["property"], fragment["isAscending"]);
    orderings.emplace_back(std::move(ordering));
}


void JsonQueryParser::build_compare(nlohmann::json fragment, std::vector<std::unique_ptr<AtomPredNode>>& preds)
{
    auto left = get_value_node(fragment["left"]);
    auto right = get_value_node(fragment["right"]);
    if (fragment["kind"] == "eq") {
        auto eq = std::make_unique<EqualityNode>(std::move(left), CompareNode::EQUAL, std::move(right));
        preds.emplace_back(std::move(eq));
    }
    else if (fragment["kind"] == "neq") {
        auto neq = std::make_unique<EqualityNode>(std::move(left), CompareNode::NOT_EQUAL, std::move(right));
        preds.emplace_back(std::move(neq));
    }
    else if (fragment["kind"] == "gt") {
        auto gt = std::make_unique<RelationalNode>(std::move(left), CompareNode::GREATER, std::move(right));
        preds.emplace_back(std::move(gt));
    }
    else if (fragment["kind"] == "gte") {
        auto gte = std::make_unique<RelationalNode>(std::move(left), CompareNode::GREATER_EQUAL, std::move(right));
        preds.emplace_back(std::move(gte));
    }
    else if (fragment["kind"] == "lt") {
        auto lt = std::make_unique<RelationalNode>(std::move(left), CompareNode::LESS, std::move(right));
        preds.emplace_back(std::move(lt));
    }
    else if (fragment["kind"] == "lte") {
        auto lte = std::make_unique<RelationalNode>(std::move(left), CompareNode::LESS_EQUAL, std::move(right));
        preds.emplace_back(std::move(lte));
    }
    else if (fragment["kind"] == "beginsWith") {
        auto begins_with = std::make_unique<StringOpsNode>(std::move(left), CompareNode::BEGINSWITH, std::move(right));
        begins_with->case_sensitive = fragment["caseSensitivity"].is_null() ? true : (bool) fragment["caseSensitivity"];
        preds.emplace_back(std::move(begins_with));
    }
    else if (fragment["kind"] == "endsWith") {
        auto ends_with = std::make_unique<StringOpsNode>(std::move(left), CompareNode::ENDSWITH, std::move(right));
        ends_with->case_sensitive = fragment["caseSensitivity"].is_null() ? true : (bool) fragment["caseSensitivity"];
        preds.emplace_back(std::move(ends_with));
    }
    else if (fragment["kind"] == "contains") {
        auto contains = std::make_unique<StringOpsNode>(std::move(left), CompareNode::CONTAINS, std::move(right));
        contains->case_sensitive = fragment["caseSensitivity"].is_null() ? true : (bool) fragment["caseSensitivity"];
        preds.emplace_back(std::move(contains));
    }
    else if (fragment["kind"] == "like") {
        auto like = std::make_unique<StringOpsNode>(std::move(left), CompareNode::LIKE, std::move(right));
        like->case_sensitive = fragment["caseSensitivity"].is_null() ? true : (bool) fragment["caseSensitivity"];
        preds.emplace_back(std::move(like));
    }
    if (fragment["kind"] == "eqString") {
        auto eq = std::make_unique<EqualityNode>(std::move(left), CompareNode::EQUAL, std::move(right));
        eq->case_sensitive = fragment["caseSensitivity"].is_null() ? true : (bool) fragment["caseSensitivity"];
        preds.emplace_back(std::move(eq));
    }
}


std::unique_ptr<ValueNode> JsonQueryParser::get_value_node(nlohmann::json fragment)
{
    if (fragment["kind"] == "property") {
        auto empty_path = std::make_unique<PathNode>();
        auto prop_node = std::make_unique<PropNode>(std::move(empty_path), fragment["name"]);
        auto value_node = std::make_unique<ValueNode>(std::move(prop_node));
        return value_node;
    }
    if (fragment["kind"] == "constant") {
        std::unique_ptr<ConstantNode> const_node;
        if (fragment["value"].is_null()){
            const_node = get_constant_node(Mixed());
            return std::make_unique<ValueNode>(std::move(const_node));
        }
        if (fragment["type"] == "string") {
            std::string value = fragment["value"].get<std::string>();
            const_node = get_constant_node(value);
        }
        if (fragment["type"] == "int") {
            int value = fragment["value"].get<int>();
            const_node = get_constant_node(value);
        }
        if (fragment["type"] == "float") {
            float value = fragment["value"].get<float>();
            const_node = get_constant_node(value);
        }
        if (fragment["type"] == "long") {
            int64_t value = fragment["value"].get<int64_t>();
            const_node = get_constant_node(value);
        }
        if (fragment["type"] == "double") {
            double value = fragment["value"].get<double>();
            const_node = get_constant_node(value);
        }
        if (fragment["type"] == "bool") {
            bool value = fragment["value"].get<bool>();
            const_node = get_constant_node(value);
        }
        return std::make_unique<ValueNode>(std::move(const_node));
    }
}


std::unique_ptr<ConstantNode> JsonQueryParser::get_constant_node(realm::Mixed value)
{
    ConstantNode::Type type;
    std::string string_value;
    std::ostringstream stream;
    if (value.is_null()){
        return std::make_unique<ConstantNode>(ConstantNode::Type::NULL_VAL, "null");
    }
    switch (value.get_type()) {
        case realm::DataType::Type::String:
            type = ConstantNode::Type::STRING;
            string_value = realm::util::format("'%1'", value.get_string());
            break;
        case realm::DataType::Type::Int:
            type = ConstantNode::Type::NUMBER;
            stream << value;
            string_value = stream.str();
            break;
        case realm::DataType::Type::Double:
            type = ConstantNode::Type::FLOAT;
            string_value = std::to_string(value.get_double());
            break;
        case realm::DataType::Type::Float:
            type = ConstantNode::Type::FLOAT;
            stream << value;
            string_value = stream.str();
            break;
        case realm::DataType::Type::Bool:
            type = value.get_bool() ? ConstantNode::Type::TRUE : ConstantNode::Type::FALSE;
            string_value = "";
            break;
        default:
            stream << value;
            string_value = stream.str();
    }
    auto constant_node = std::make_unique<ConstantNode>(type, string_value);
    return constant_node;
}

} // namespace realm