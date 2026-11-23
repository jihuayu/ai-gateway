rootProject.name = "ai-gateway"

include(
    ":gateway-common",
    ":gateway-data-plane",
)

if (file("gateway-control-plane").exists()) {
    include(":gateway-control-plane")
}
