Algebra of changesets
=====================

### States and changesets

A *changeset* is a recipe for modifying a particular state and thereby producing
a new state.

**Definition:** If `A` and `B` are changesets, and the state produced by `A` is
the base state of `B`, then `C = A + B` is the changeset resulting from
concatenating `A` and `B`. I.e., `C` is the *concatenation* of `A` and `B`.

Changeset concatenation shall be assumed to be associative.

**Definition:** If `A` is a changeset whose base state is `α`, then the
application of `A` to `α`, denoted as `S(α, A)`, is the final state of `A`. The
function, `S`, is called the *changeset application function*.

It shall be assumed that the changeset application function can be evaluated
stepwise for a concatenated changeset:

    S(α, A + B)  =  S(S(α, A), B)                                            (1)

**Definition:** Two changesets `A` and `B`, having the same base state, `α`, are
*equivalent*, written as `A ~ B`, if, and onlæy if they produce the same final
state, that is, if, and only if `S(α, A) = S(α, B)`. This does not mean that `A`
and `B` are equal.

The following two rules can then be trivially derived from (1):

    (A ~ B) ∧ (C ~ D)  =>  A + C ~ B + D                                     (2)
    (A ~ B) ∧ ((A + C) ~ (B + D))  =>  C ~ D                                 (3)


### Operational transformation

**Definition:** A function `T(A, B)`, where `A` and `B` are changesets with the
same base state, is a *changeset transformation function* if it produces a
changeset that is based on the final state of `B`. Note that this corresponds to
a *rebase operation* in a distributed version control system such as Git.

**Definition:** If `A` and `B` are changesets with the same base state, then the
*serialization* of `A` and `B` with respect to the changeset transformation
function `T`, written as `A|B`, is `A + T(B, A)`.

Serialization is not necessarily commutative, nor is it necessarily associative.

In notation, the serialization operator associates to the left, so `A|B|C` means
`(A|B)|C`, and it has higher precedence than the concatenation operator, so `A +
B|C` means `A + (B|C)`.

**Definition:** A changeset transformation function is *first-order convergent*
if `A|B ~ B|A` for any two changesets `A` and `B` with the same base state.

**Definition:** A changeset transformation function `T` is *soluble* if both of
the following are true for any three changesets `A`, `B`, and `C` such that `B`
is based on the final state of `A`, and `A` and `C` have the same base state:

    T(A + B, C) = T(A, C) + T(B, T(C, A))                                    (4)
    T(C, A + B) = T(T(C, A), B)                                              (5)

A changeset transformation function is called an *operational transformation* if
it is first-order convergent, is soluble, and it is generally the case that the
effect of `T(A, B)` preserves the intent of `A`.


### Second-order convergence

**Definition:** A first-order convergent changeset transformation function is
also *second-order convergent* if `A|B|C ~ B|A|C` for any three changesets `A`,
`B`, and `C` with the same base state.

**Theorem:** A first-order convergent changeset transformation function is
second-order convergent if `B ~ C => T(A, B) ~ T(A, C)` for any three changesets
`A`, `B`, and `C` with the same base state.

**Proof:** With a bit of trivial renaming, we get:

    D ~ E => T(C, D) ~ T(C, E)

If we then substitute `A|B` for `D` and `B|A` for `E`, we get:

    A|B ~ B|A  =>  T(C, A|B) ~ T(C, B|A)

The left side of the implication is guaranteed by the assumption of first-order
convergence. This leaves us with just the right side. After expansion using (2),
we get:

    A|B + T(C, A|B)  ~  B|A + T(C, B|A)

However, by the definition of serialization we get:

    A|B|C  ~  B|A|C

Note that this does not prove that any particular transformation function is
second-order convergent, just that **if** it satisfies `B ~ C => T(A, B) ~ T(A,
C)` for any three changesets `A`, `B`, and `C`, **then** it is second-order
convergent.
