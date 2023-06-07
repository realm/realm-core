import { CppVar, CppFunc, CppDecls } from "@realm/bindgen/cpp";
import { bindModel, Type, Template, Const, Ref, Func, Arg } from "@realm/bindgen/bound-model";
import { TemplateContext } from "@realm/bindgen/context";
import { clangFormat } from "@realm/bindgen/formatter";

// This is a good starting place but it is incomplete. It only checks convertibility, not for correct types.
// In some cases this is OK. Especially SDK -> C++. However, for C++ -> SDK, we often need more precise types,
// eg because we call methods on them. This is mostly an issue for primitives and templates. It is _especially_
// a problem for types like Nullable<> which aren't reflected directly in the C++ types, but require that types
// used as arguments support a specific API. We should also ensure that numeric types have the right sign and width.

function funcToStdFunction(sig: Type): Type {
  if (sig.kind != "Func") return sig;
  return new Template("std::function", [
    new Func(
      funcToStdFunction(sig.ret),
      sig.args.map((a) => new Arg(a.name, funcToStdFunction(a.type))),
      sig.isConst,
      sig.noexcept,
      sig.isOffThread,
    ),
  ]);
}

function requiresExactReturn(ret: Type) {
  ret = ret.removeConstRef();

  // We require that anything returning a view (including references) must return that exact type.
  // This is to ensure that the lifetime of buffers are long enough to survive returning to the SDK,
  // without copying to a separate buffer. std::string is checked to ensure that we declare things
  // as views when we should, and because `const string&` is a kind of view.
  if (ret.kind == "Primitive") {
    return ["StringData", "std::string_view", "std::string", "BinaryData", "Mixed"].includes(ret.name);
  }

  if (ret.kind == "Template") {
    // Optional is convertible from T, so make sure we aren't marking things optional incorrectly.
    // TODO remove this once we check API rather than convertibility.
    if (ret.name == "util::Optional") return true;
    return ret.args.some(requiresExactReturn);
  }
  return false;
}

export function generate({ spec, file: makeFile }: TemplateContext): void {
  const out = makeFile("validate_spec.cpp", clangFormat);

  // HEADER
  out(`// This file is generated: Update the spec instead of editing this file directly`);

  for (const header of spec.headers) {
    out(`#include <${header}>`);
  }

  out(`namespace realm::validate_spec {`);

  const model = bindModel(spec);
  const decls = new CppDecls();

  for (const enm of model.enums) {
    decls.static_asserts.push(`std::is_enum_v<${enm.cppName}>`);
    decls.static_asserts.push(`sizeof(${enm.cppName}) <= sizeof(int32_t), "we only support enums up to 32 bits"`);
    for (const { name, value } of enm.enumerators) {
      decls.static_asserts.push(`${enm.cppName}(int(${value})) == ${enm.cppName}::${name}`);
    }
  }

  for (const rec of model.records) {
    decls.free_funcs.push(
      new CppFunc(
        `ensure_can_consume_${rec.name}`,
        "void",
        [new CppVar(`[[maybe_unused]] const ${rec.cppName}&`, "rec")],
        {
          body: `${rec.fields
            .filter((f) => !f.type.isFunction()) // Functions on structs only go _from_ the sdk to core.
            .map((f) => `[[maybe_unused]] ${f.type.toCpp()} ${f.name} = rec.${f.cppName};`)
            .join("\n")}`,
        },
      ),
    );

    // SyncError only gets converted to the SDK, so it doesn't need to be default constructable.
    // TODO stop hard coding here. Make this specified in spec or detect automatically like a binding does.
    if (rec.cppName == "SyncError") {
      continue;
    }

    decls.free_funcs.push(
      new CppFunc(
        `ensure_can_produce_${rec.name}`,
        rec.cppName,
        rec.fields.map((f) => new CppVar(f.type.toCpp(), f.name)),
        {
          body: `
            auto out = ${rec.cppName}();
            ${rec.fields
              .filter((f) => !f.cppName.endsWith("()")) // These are function calls to send data _to_ the sdk.
              .map((f) => `out.${f.cppName} = FWD(${f.name});`)
              .join("\n")}
            return out;
          `,
        },
      ),
    );
  }

  for (const cls of model.classes) {
    const self = cls.needsDeref ? "(*_this)" : "_this";
    for (const method of cls.methods) {
      const ret = method.sig.ret;
      const ignoreReturn =
        // We allow declaring void returns for functions that return values we don't need.
        ret.isVoid() ||
        // TODO these return pairs with iterators that we model as pointers. That doesn't work with the lazy validation
        // we are doing, will need better validation.
        (cls.name == "MutableSyncSubscriptionSet" && method.name == "insert_or_assign");

      const args = method.sig.args //
        .map((a) => new CppVar(funcToStdFunction(a.type).toCpp(), a.name));
      if (!method.isStatic) {
        let thisType: Type = method.sig.isConst ? new Const(cls) : cls;
        if (cls.sharedPtrWrapped) {
          thisType = new Template("std::shared_ptr", [thisType]);
        }
        thisType = new Ref(thisType);
        args.unshift(new CppVar(thisType.toCpp(), "_this"));
      }
      const callFunction = method.call({ self }, ...method.sig.args.map((a) => `FWD(${a.name})`));

      let staticAsserts = "";
      if (requiresExactReturn(ret)) {
        staticAsserts += `static_assert(std::is_same_v<decltype(${callFunction}), ${ret.toCpp()}>);`;
      }

      decls.free_funcs.push(
        new CppFunc(`ensure_can_call_${method.id}`, ignoreReturn ? "void" : ret.toCpp(), args, {
          body: `${staticAsserts} ${ignoreReturn ? "" : "return "} ${callFunction}; `,
        }),
      );
    }
  }

  decls.outputDefsTo(out);

  out(`
    // Make it seem like all functions will be used by escaping pointers to them.
    void* volatile escape;
    void use_all_methods() {
      ${decls.free_funcs.map((f) => `escape = reinterpret_cast<void*>(&${f.name});`).join("\n")}
    }

    } // namespace realm::js::node
  `);
}
