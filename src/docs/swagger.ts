import swaggerJsdoc from "swagger-jsdoc";

const options: swaggerJsdoc.Options = {
    definition: {
        openapi: "3.0.0",
        info: {
            title: "Crypto Watch API",
            version: "1.0.0",
            description: "API documentation for Crypto Watch",
        },
        servers: [{
            url: "/api",
            description: "API server",
        }],
    },
    apis: ["./src/modules/**/*.ts"],
};

export const swaggerSpec = swaggerJsdoc(options);