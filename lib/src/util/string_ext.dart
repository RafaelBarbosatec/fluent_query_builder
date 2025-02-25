extension StringExt on String {
  dynamic convertToType() {
    if (isEmpty) {
      return '';
    }

    if (trim() == 'null') {
      return null;
    }
    return int.tryParse(this) ?? double.tryParse(this) ?? this;
  }
}
