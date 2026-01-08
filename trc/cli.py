"""Main CLI entrypoint for TRC."""
import click
from trc.commands import auth, user, ls


@click.group()
def main():
    """Trossen Robotics Cloud CLI."""
    pass


# Register command groups and commands
main.add_command(auth.auth)
main.add_command(user.user)
main.add_command(ls.ls)


if __name__ == '__main__':
    main()
