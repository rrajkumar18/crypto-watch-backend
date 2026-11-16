import swaggerJsdoc from "swagger-jsdoc";

const options: swaggerJsdoc.Options = {
    definition: {
        openapi: "3.0.0",
        info: {
            title: "Crypto Watch API",
            version: "1.0.0",
            description: "API documentation for Crypto Watch",
        },
        servers: [
            {
                url: "/api",
                description: "API server",
            },
        ],

        // Reusable schemas must be inside definition
        components: {
            schemas: {
                MarketSession: {
                    type: "object",
                    properties: {
                        id: {
                            type: "string",
                            format: "uuid",
                        },
                        mode: {
                            type: "string",
                            enum: ["MANUAL", "SCHEDULED"],
                        },
                        status: {
                            type: "string",
                            example: "RUNNING",
                        },
                        startedAt: {
                            type: "string",
                            format: "date-time",
                        },
                        scheduledStopAt: {
                            type: "string",
                            format: "date-time",
                        },
                    },
                },

                ApiError: {
                    type: "object",
                    properties: {
                        message: {
                            type: "string",
                            example: "A collection session is already running",
                        },
                    },
                },
            },
        },
    },

    apis: ["./src/modules/**/*.ts"],
};

export const swaggerSpec = swaggerJsdoc(options);