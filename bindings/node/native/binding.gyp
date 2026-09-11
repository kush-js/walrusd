{
  "targets": [
    {
      "target_name": "walrusd",
      "sources": ["walrusd.cc"],
      "defines": ["NAPI_DISABLE_CPP_EXCEPTIONS"],
      "cflags!": ["-fno-exceptions"],
      "cflags_cc!": ["-fno-exceptions"]
    }
  ]
}
