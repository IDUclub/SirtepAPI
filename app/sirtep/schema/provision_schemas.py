from pydantic import BaseModel, model_validator


class ProvisionSchema(BaseModel):
    """
    Schema for provision response for tep calculation.
    Attributes:
        periods (list[int]): List of periods.
        provision (list[dict[str, float]]): List of provision dictionaries.
    """

    periods: list[int]
    provision: list[dict[str, float]]

    @model_validator(mode="after")
    @classmethod
    def check_lengths(cls, model):
        """
        Validates that the lengths of 'periods' and 'provision' lists are equal.
        Args:
            model (ProvisionSchema): The instance of ProvisionSchema to validate.
        Returns:
            ProvisionSchema: The validated instance.
        Raises:
            ValueError: If the lengths of 'periods' and 'provision' do not
                        match.
        """

        if len(model.periods) != len(model.provision):
            raise ValueError("The lengths of 'periods' and 'provision' must be equal.")
        return model


class ProvisionInProgressSchema(BaseModel):
    """
    Schema for tracking the progress of provision calculation.
    Attributes:
        status (str): The current status of the provision calculation.
        progress (float | None): The progress percentage of the calculation.
        message (str | None): Additional message or information about the status.
    """

    status: str
    progress: float | None
    message: str | None
