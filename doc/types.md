# Adding supported key/path types

Types are handled in the `codax.pathwise` namespace. The map at the end of the file is used to generate the encoding and decoding functions and should be used for reference.

Adding a type requires 4 things:

1. a unique (but arbitrary) ordering hex tag (single ascii char) which identifies the type and determines where it fits in the total ordering of the underlying b+ tree. This should be chosen in a sensible way with reference to existing types.
2. a predicate fn to determine if the value is of that type
3. an encoding function which turns values of that type into strings whose lexicographical order matches their logical order (for example, in the case of dates, ensuring that preceding dates sort before subsequent dates)
4. a decoding function which takes the string representation (actually a sequence of characters which must be `str/join`ed, for arcane reasons) and instantiates an object of the appropriate type.
